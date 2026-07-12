from __future__ import annotations

import csv
import io
import json
import math
import shutil
import tempfile
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ai_trading_system.config import (
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import validate_data_cache, write_data_quality_report
from ai_trading_system.etf_portfolio import dynamic_v3_paper_tracking
from ai_trading_system.etf_portfolio.data import load_standard_prices
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DEFAULT_BACKFILL_REPAIR_DIR,
    DEFAULT_BACKFILLED_OUTCOME_DIR,
    DEFAULT_HISTORICAL_PAPER_SIM_DIR,
    DEFAULT_HISTORICAL_REPLAY_DIR,
    DEFAULT_REPLAY_DIAGNOSIS_DIR,
    DEFAULT_REPLAY_INVENTORY_DIR,
    validate_backfill_outcome_artifact,
    validate_backfill_repair_artifact,
    validate_historical_paper_sim_artifact,
    validate_historical_replay_artifact,
    validate_replay_diagnosis_artifact,
    validate_replay_inventory_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DEFAULT_ADVISORY_OUTCOME_DIR,
    DEFAULT_OWNER_ATTRIBUTION_DIR,
    DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    DEFAULT_PAPER_PORTFOLIO_DIR,
    DEFAULT_RATES_CACHE_PATH,
    DEFAULT_SHADOW_AGING_DIR,
    DEFAULT_SHADOW_SHORTLIST_DIR,
    DEFAULT_WEEKLY_ADVISORY_REVIEW_DIR,
    run_owner_attribution,
    run_shadow_aging,
    run_weekly_advisory_review,
    update_advisory_outcome,
    validate_advisory_outcome_artifact,
    validate_owner_attribution_artifact,
    validate_shadow_aging_artifact,
    validate_weekly_advisory_review_artifact,
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
    validate_owner_review_artifact,
    validate_position_advisory_daily_artifact,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH, load_etf_config_bundle

DEFAULT_OUTCOME_DUE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "outcome_due"
DEFAULT_REPLAY_SAMPLE_EXPANSION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "replay_sample_expansion"
DEFAULT_OUTCOME_DASHBOARD_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "outcome_dashboard"
DEFAULT_LIMITED_VS_NOTRADE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "limited_vs_notrade"
DEFAULT_CONSENSUS_RISK_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "consensus_risk"
DEFAULT_OUTCOME_UPDATE_REVIEW_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "outcome_update_review"
DEFAULT_OUTCOME_UPDATE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "outcome_update"
DEFAULT_ROLLING_EVIDENCE_REFRESH_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "rolling_evidence_refresh"
DEFAULT_EVIDENCE_TREND_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "evidence_trend"
DEFAULT_EVIDENCE_TREND_POLICY_PATH = Path(
    "config/etf_portfolio/dynamic_v3_rescue/evidence_trend_v1.yaml"
)
DEFAULT_FORWARD_OUTCOME_DECISION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "forward_outcome_decision"
DEFAULT_FORWARD_OUTCOME_DECISION_POLICY_PATH = Path(
    "config/etf_portfolio/dynamic_v3_rescue/forward_outcome_decision_v1.yaml"
)
OUTCOME_DUE_SNAPSHOT_SCHEMA_VERSION = "outcome_due_source_snapshot.v2"
REPLAY_SAMPLE_EXPANSION_SNAPSHOT_SCHEMA_VERSION = "replay_sample_expansion_source_snapshot.v2"
DEFAULT_REPLAY_SAMPLE_EXPANSION_POLICY_PATH = Path(
    "config/etf_portfolio/dynamic_v3_rescue/replay_sample_expansion_v1.yaml"
)
OUTCOME_DASHBOARD_SNAPSHOT_SCHEMA_VERSION = "outcome_dashboard_source_snapshot.v2"
DEFAULT_OUTCOME_DASHBOARD_POLICY_PATH = Path(
    "config/etf_portfolio/dynamic_v3_rescue/outcome_dashboard_v1.yaml"
)
LIMITED_VS_NOTRADE_SNAPSHOT_SCHEMA_VERSION = "limited_vs_notrade_source_snapshot.v2"
DEFAULT_LIMITED_VS_NOTRADE_POLICY_PATH = Path(
    "config/etf_portfolio/dynamic_v3_rescue/limited_vs_notrade_v1.yaml"
)
CONSENSUS_RISK_SNAPSHOT_SCHEMA_VERSION = "consensus_risk_source_snapshot.v2"
DEFAULT_CONSENSUS_RISK_POLICY_PATH = Path(
    "config/etf_portfolio/dynamic_v3_rescue/consensus_risk_v1.yaml"
)
OUTCOME_UPDATE_REVIEW_SNAPSHOT_SCHEMA_VERSION = "outcome_update_review_source_snapshot.v2"
OUTCOME_UPDATE_SNAPSHOT_SCHEMA_VERSION = "outcome_update_source_snapshot.v2"
OUTCOME_UPDATE_TRANSACTION_SCHEMA_VERSION = "outcome_update_transaction.v1"
ROLLING_EVIDENCE_REFRESH_SNAPSHOT_SCHEMA_VERSION = "rolling_evidence_refresh_source_snapshot.v2"
ROLLING_EVIDENCE_REFRESH_TRANSACTION_SCHEMA_VERSION = "rolling_evidence_refresh_transaction.v1"
EVIDENCE_TREND_SNAPSHOT_SCHEMA_VERSION = "evidence_trend_source_snapshot.v2"
FORWARD_OUTCOME_DECISION_SNAPSHOT_SCHEMA_VERSION = "forward_outcome_decision_source_snapshot.v2"

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
OUTCOME_UPDATE_REVIEW_STATUSES = {"READY_TO_UPDATE", "NEEDS_REVIEW", "BLOCKED"}
OUTCOME_UPDATE_SKIP_REASONS = {
    "NOT_DUE",
    "PRICE_MISSING",
    "BLOCKED_BY_REVIEW",
    "INSUFFICIENT_DATA",
}
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
    generated = _aware_utc(generated_at or datetime.now(UTC), "generated_at")
    if as_of > generated.date():
        raise DynamicV3OutcomeAccumulationError("outcome due as_of cannot be in the future")
    due_id = _stable_id("outcome-due", as_of.isoformat(), generated.isoformat())
    price_index = {
        symbol: {item for item in dates if item <= as_of}
        for symbol, dates in _price_availability_index(prices_path).items()
    }
    latest_price_date = _latest_price_date_on_or_before(price_index, as_of)
    price_dates = _all_price_dates(price_index)
    quality = _cached_data_quality_result(
        as_of=as_of,
        prices_path=prices_path,
        rates_path=rates_path,
        enforce=enforce_data_quality_gate,
    )
    source_bundles: dict[str, Any] = {}
    identities: set[tuple[str, int]] = set()
    inventory: list[dict[str, Any]] = []
    for outcome_dir in _artifact_children(advisory_outcome_dir):
        outcome_id = outcome_dir.name
        validation = validate_advisory_outcome_artifact(
            outcome_id=outcome_id, output_dir=advisory_outcome_dir
        )
        if validation.get("status") != "PASS":
            raise DynamicV3OutcomeAccumulationError(
                f"advisory outcome validation must PASS: {outcome_id}"
            )
        bundle = _immutable_source_bundle(outcome_dir)
        manifest = _mapping(_source_bundle_content(bundle, "advisory_outcome_manifest.json"))
        event = _mapping(_source_bundle_content(bundle, "advisory_event.json"))
        windows = _records(_source_bundle_content(bundle, "outcome_windows.jsonl"))
        source_time = _datetime_from_any(manifest.get("updated_at") or manifest.get("generated_at"))
        if source_time is None or source_time > generated:
            raise DynamicV3OutcomeAccumulationError(
                f"advisory outcome source time is invalid or future: {outcome_id}"
            )
        for row in windows:
            identity = (
                _text(row.get("daily_advisory_id") or manifest.get("daily_advisory_id")),
                _int(row.get("window_days")),
            )
            if not identity[0] or identity[1] not in OUTCOME_WINDOWS or identity in identities:
                raise DynamicV3OutcomeAccumulationError(
                    "duplicate or invalid daily-advisory outcome window identity"
                )
            identities.add(identity)
            inventory.append(
                _due_inventory_row(
                    outcome_id=_text(manifest.get("outcome_id"), outcome_id),
                    manifest=manifest,
                    event=event,
                    window=row,
                    price_index=price_index,
                    price_dates=price_dates,
                    latest_price_date=latest_price_date,
                )
            )
        source_bundles[outcome_id] = bundle
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
    due_dir = _unique_dir(output_dir / due_id)
    due_dir.mkdir(parents=True, exist_ok=False)
    quality_report_path = ""
    if quality is not None:
        quality_report = due_dir / "validate_data_quality_report.md"
        write_data_quality_report(quality, quality_report)
        quality_report_path = str(quality_report)
        quality_status = quality.status
    else:
        quality_status = "SKIPPED_EXPLICIT_TEST_FIXTURE"
    snapshot = {
        "schema_version": OUTCOME_DUE_SNAPSHOT_SCHEMA_VERSION,
        "due_id": due_dir.name,
        "generated_at": generated.isoformat(),
        "as_of": as_of.isoformat(),
        "advisory_outcome_dir": str(advisory_outcome_dir),
        "source_bundles": source_bundles,
        "source_validation_statuses": {outcome_id: "PASS" for outcome_id in source_bundles},
        "prices_path": str(prices_path),
        "prices_checksum": _file_sha256(prices_path),
        "rates_path": str(rates_path),
        "rates_checksum": _file_sha256(rates_path),
        "price_date_availability": {
            symbol: [item.isoformat() for item in sorted(dates) if item <= as_of]
            for symbol, dates in sorted(price_index.items())
        },
        "data_quality_status": quality_status,
        "data_quality_report_path": quality_report_path,
        "data_quality_report_checksum": (
            _file_sha256(Path(quality_report_path)) if quality_report_path else ""
        ),
        "production_effect": "none",
    }
    snapshot_path = due_dir / "outcome_due_source_snapshot.json"
    _write_json(snapshot_path, snapshot)
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
        "source_snapshot_path": str(snapshot_path),
        "source_snapshot_checksum": _file_sha256(snapshot_path),
        "advisory_outcome_dir": str(advisory_outcome_dir),
        "prices_path": str(prices_path),
        "outcome_due_manifest_path": str(due_dir / "outcome_due_manifest.json"),
        "due_window_inventory_path": str(due_dir / "due_window_inventory.jsonl"),
        "pending_window_summary_path": str(due_dir / "pending_window_summary.json"),
        "update_ready_list_path": str(due_dir / "update_ready_list.json"),
        "outcome_due_report_path": str(due_dir / "outcome_due_report.md"),
        "update_ready_execution_path": str(due_dir / "update_ready_execution.json"),
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
    generated = _aware_utc(generated_at or datetime.now(UTC), "generated_at")
    due_dir = output_dir / due_id
    if (due_dir / "update_ready_execution.json").exists():
        raise DynamicV3OutcomeAccumulationError("outcome due update-ready already executed")
    due_validation = validate_outcome_due_artifact(due_id=due_id, output_dir=output_dir)
    if due_validation.get("status") != "PASS":
        raise DynamicV3OutcomeAccumulationError(
            f"outcome due validation must PASS before update: {due_validation.get('status')}"
        )
    manifest = _read_json(due_dir / "outcome_due_manifest.json")
    source_generated = _datetime_from_any(manifest.get("generated_at"))
    if source_generated is None or generated < source_generated:
        raise DynamicV3OutcomeAccumulationError(
            "outcome due update generated_at must not precede scan"
        )
    as_of = _date_from_any(manifest.get("as_of"))
    if as_of is None:
        raise DynamicV3OutcomeAccumulationError("outcome due manifest missing as_of")
    ready_payload = _read_json(due_dir / "update_ready_list.json")
    ready_rows = [
        row for row in _records(ready_payload.get("update_ready")) if row.get("can_update")
    ]
    windows_by_outcome: dict[str, set[int]] = defaultdict(set)
    for row in ready_rows:
        if row.get("due_status") != "DUE":
            raise DynamicV3OutcomeAccumulationError("update-ready contains non-DUE row")
        outcome_id = _text(row.get("outcome_id"))
        window_days = _int(row.get("window_days"))
        if not outcome_id or window_days not in OUTCOME_WINDOWS:
            raise DynamicV3OutcomeAccumulationError("update-ready identity is invalid")
        windows_by_outcome[outcome_id].add(window_days)
    updated = []
    for outcome_id, allowed_windows in sorted(windows_by_outcome.items()):
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
            allowed_window_days=allowed_windows,
        )
        post_validation = validate_advisory_outcome_artifact(
            outcome_id=outcome_id, output_dir=advisory_outcome_dir
        )
        if post_validation.get("status") != "PASS":
            raise DynamicV3OutcomeAccumulationError(
                f"updated advisory outcome validation failed: {outcome_id}"
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
                "allowed_window_days": sorted(allowed_windows),
                "post_update_validation_status": "PASS",
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
        "source_due_validation_status": "PASS",
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
    summary = _read_optional_json(due_dir / "pending_window_summary.json") or {}
    ready = _records(
        (_read_optional_json(due_dir / "update_ready_list.json") or {}).get("update_ready")
    )
    shallow_checks = [
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
    snapshot_path = due_dir / "outcome_due_source_snapshot.json"
    if not snapshot_path.is_file():
        payload = _validation_payload(
            report_type="etf_dynamic_v3_outcome_due_validation",
            artifact_id_key="due_id",
            artifact_id=due_id,
            checks=shallow_checks,
        )
        if payload["status"] == "PASS":
            payload["status"] = "PASS_WITH_WARNINGS"
        payload["source_snapshot_status"] = "LEGACY_UNSNAPSHOTTED"
        return payload
    snapshot = _read_optional_json(snapshot_path) or {}
    bundles = _mapping(snapshot.get("source_bundles"))
    try:
        as_of = _date_from_any(snapshot.get("as_of"))
        generated = _datetime_from_any(snapshot.get("generated_at"))
        if as_of is None or generated is None or as_of > generated.date():
            raise DynamicV3OutcomeAccumulationError("snapshot time fields are invalid")
        price_index = {
            symbol: {
                parsed for value in _texts(values) if (parsed := _date_from_any(value)) is not None
            }
            for symbol, values in _mapping(snapshot.get("price_date_availability")).items()
        }
        latest_price_date = _latest_price_date_on_or_before(price_index, as_of)
        price_dates = _all_price_dates(price_index)
        expected_rows: list[dict[str, Any]] = []
        identities: set[tuple[str, int]] = set()
        for outcome_id, raw_bundle in sorted(bundles.items()):
            bundle = _mapping(raw_bundle)
            source_manifest = _mapping(
                _source_bundle_content(bundle, "advisory_outcome_manifest.json")
            )
            event = _mapping(_source_bundle_content(bundle, "advisory_event.json"))
            windows = _records(_source_bundle_content(bundle, "outcome_windows.jsonl"))
            for window in windows:
                identity = (
                    _text(
                        window.get("daily_advisory_id") or source_manifest.get("daily_advisory_id")
                    ),
                    _int(window.get("window_days")),
                )
                if not identity[0] or identity[1] not in OUTCOME_WINDOWS or identity in identities:
                    raise DynamicV3OutcomeAccumulationError("snapshot outcome identity is invalid")
                identities.add(identity)
                expected_rows.append(
                    _due_inventory_row(
                        outcome_id=outcome_id,
                        manifest=source_manifest,
                        event=event,
                        window=window,
                        price_index=price_index,
                        price_dates=price_dates,
                        latest_price_date=latest_price_date,
                    )
                )
        expected_rows = sorted(
            expected_rows,
            key=lambda row: (
                _text(row.get("as_of")),
                _text(row.get("daily_advisory_id")),
                _int(row.get("window_days")),
                _text(row.get("outcome_id")),
            ),
        )
        expected_ready = [row for row in expected_rows if row.get("can_update") is True]
        expected_summary = _pending_window_summary(
            expected_rows, as_of=as_of, latest_price_date=latest_price_date
        )
        expected_status = "PASS"
        if not expected_rows:
            expected_status = "INSUFFICIENT_DATA"
        elif expected_summary["price_missing_windows"]:
            expected_status = "PASS_WITH_WARNINGS"
        expected_manifest_fields = {
            "due_id": due_id,
            "generated_at": snapshot.get("generated_at"),
            "as_of": snapshot.get("as_of"),
            "status": expected_status,
            "latest_price_date": (
                "" if latest_price_date is None else latest_price_date.isoformat()
            ),
            "data_quality_status": snapshot.get("data_quality_status"),
            "update_ready_count": expected_summary["update_ready_count"],
        }
        expected_report = render_outcome_due_report(
            {**manifest, **expected_manifest_fields}, expected_summary, expected_ready
        )
        live_sources_match = all(
            _source_bundle_matches(_mapping(bundle)) for bundle in bundles.values()
        )
        prices_path = Path(_text(snapshot.get("prices_path")))
        rates_path = Path(_text(snapshot.get("rates_path")))
        input_checksums_match = (
            prices_path.is_file()
            and rates_path.is_file()
            and snapshot.get("prices_checksum") == _file_sha256(prices_path)
            and snapshot.get("rates_checksum") == _file_sha256(rates_path)
        )
        report_path = Path(_text(snapshot.get("data_quality_report_path")))
        dq_evidence_matches = snapshot.get(
            "data_quality_status"
        ) == "SKIPPED_EXPLICIT_TEST_FIXTURE" or (
            report_path.is_file()
            and snapshot.get("data_quality_report_checksum") == _file_sha256(report_path)
        )
        recompute_error = ""
    except Exception as exc:  # noqa: BLE001
        expected_rows, expected_ready, expected_summary = [], [], {}
        expected_manifest_fields, expected_report = {}, ""
        live_sources_match = input_checksums_match = dq_evidence_matches = False
        recompute_error = str(exc)
    checks = [
        *shallow_checks,
        _check(
            "source_snapshot_schema_valid",
            snapshot.get("schema_version") == OUTCOME_DUE_SNAPSHOT_SCHEMA_VERSION,
            OUTCOME_DUE_SNAPSHOT_SCHEMA_VERSION,
        ),
        _check(
            "source_snapshot_checksum_matches",
            manifest.get("source_snapshot_checksum") == _file_sha256(snapshot_path),
            "outcome due snapshot",
        ),
        _check("source_files_unchanged", live_sources_match, "advisory outcome bundles"),
        _check("cached_inputs_unchanged", input_checksums_match, "prices and rates"),
        _check("data_quality_evidence_matches", dq_evidence_matches, "DQ report"),
        _check("inventory_recomputed", rows == expected_rows, recompute_error),
        _check("summary_recomputed", summary == expected_summary, recompute_error),
        _check("update_ready_recomputed", ready == expected_ready, recompute_error),
        _check(
            "manifest_derived_fields_match",
            all(manifest.get(key) == value for key, value in expected_manifest_fields.items()),
            recompute_error,
        ),
        _check(
            "report_recomputed",
            (due_dir / "outcome_due_report.md").is_file()
            and (due_dir / "outcome_due_report.md").read_text(encoding="utf-8") == expected_report,
            "Markdown report",
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
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    policy_path: Path = DEFAULT_REPLAY_SAMPLE_EXPANSION_POLICY_PATH,
    enforce_data_quality_gate: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _aware_utc(generated_at or datetime.now(UTC), "generated_at")
    if start > end:
        raise DynamicV3OutcomeAccumulationError("replay expansion start must be <= end")
    if end > generated.date():
        raise DynamicV3OutcomeAccumulationError("replay expansion end cannot be in the future")
    policy = _load_replay_sample_expansion_policy(policy_path)
    quality = _cached_data_quality_result(
        as_of=end,
        prices_path=prices_path,
        rates_path=rates_path,
        enforce=enforce_data_quality_gate,
    )
    snapshot = _build_replay_sample_expansion_snapshot(
        start=start,
        end=end,
        generated=generated,
        daily_advisory_dir=daily_advisory_dir,
        owner_review_dir=owner_review_dir,
        replay_inventory_dir=replay_inventory_dir,
        prices_path=prices_path,
        rates_path=rates_path,
        policy_path=policy_path,
        policy=policy,
        quality_status=(quality.status if quality is not None else "SKIPPED_EXPLICIT_TEST_FIXTURE"),
    )
    source_inventory, rows = _replay_sample_expansion_views_from_snapshot(snapshot)
    summary = _pit_classification_summary(rows)
    expansion_id = _stable_id(
        "replay-sample-expansion",
        start.isoformat(),
        end.isoformat(),
        generated.isoformat(),
    )
    expansion_dir = _unique_dir(output_dir / expansion_id)
    expansion_dir.mkdir(parents=True, exist_ok=False)
    quality_report_path = ""
    if quality is not None:
        quality_report = expansion_dir / "validate_data_quality_report.md"
        write_data_quality_report(quality, quality_report)
        quality_report_path = str(quality_report)
    snapshot["expansion_id"] = expansion_dir.name
    snapshot["data_quality_report_path"] = quality_report_path
    snapshot["data_quality_report_checksum"] = (
        _file_sha256(Path(quality_report_path)) if quality_report_path else ""
    )
    snapshot_path = expansion_dir / "replay_sample_expansion_source_snapshot.json"
    _write_json(snapshot_path, snapshot)
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
        "market_regime": "ai_after_chatgpt",
        "data_quality_status": snapshot["data_quality_status"],
        "data_quality_report_path": quality_report_path,
        "source_snapshot_path": str(snapshot_path),
        "source_snapshot_checksum": _file_sha256(snapshot_path),
        "policy_id": policy["policy_id"],
        "policy_version": policy["version"],
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
        "source_snapshot_artifact_path": str(snapshot_path),
        "replay_sample_expansion_report_path": str(
            expansion_dir / "replay_sample_expansion_report.md"
        ),
        "pit_unsafe_allowed_in_default_replay": False,
        "automatic_replay_execution_allowed": False,
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
        "source_snapshot": snapshot,
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
    inventory = _read_jsonl(expansion_dir / "candidate_source_inventory.jsonl")
    rows = _read_jsonl(expansion_dir / "expanded_replay_events.jsonl")
    summary = _read_optional_json(expansion_dir / "pit_classification_summary.json") or {}
    snapshot_path = expansion_dir / "replay_sample_expansion_source_snapshot.json"
    if not snapshot_path.is_file():
        legacy_checks = [
            _check(
                "manifest_exists",
                (expansion_dir / "expansion_manifest.json").is_file(),
                expansion_id,
            ),
            _check(
                "source_inventory_exists",
                (expansion_dir / "candidate_source_inventory.jsonl").is_file(),
                expansion_id,
            ),
            _check(
                "expanded_events_exists",
                (expansion_dir / "expanded_replay_events.jsonl").is_file(),
                expansion_id,
            ),
            _check(
                "pit_summary_exists",
                (expansion_dir / "pit_classification_summary.json").is_file(),
                expansion_id,
            ),
            _check(
                "report_exists",
                (expansion_dir / "replay_sample_expansion_report.md").is_file(),
                expansion_id,
            ),
            _check(
                "pit_unsafe_default_excluded",
                all(
                    row.get("replay_eligibility") == "INELIGIBLE"
                    for row in rows
                    if row.get("pit_safety_status") == "PIT_UNSAFE"
                ),
                "legacy shallow validation",
            ),
        ]
        payload = _validation_payload(
            report_type="etf_dynamic_v3_replay_sample_expansion_validation",
            artifact_id_key="expansion_id",
            artifact_id=expansion_id,
            checks=legacy_checks,
        )
        if payload["status"] == "PASS":
            payload["status"] = "PASS_WITH_WARNINGS"
        payload["source_snapshot_status"] = "LEGACY_UNSNAPSHOTTED"
        return payload
    snapshot = _read_optional_json(snapshot_path) or {}
    recompute_error = ""
    try:
        expected_inventory, expected_rows = _replay_sample_expansion_views_from_snapshot(snapshot)
        expected_summary = _pit_classification_summary(expected_rows)
    except Exception as exc:  # noqa: BLE001
        recompute_error = str(exc)
        expected_inventory, expected_rows, expected_summary = [], [], {}
    expected_status = "PASS"
    if not expected_rows:
        expected_status = "INSUFFICIENT_DATA"
    elif expected_summary.get("pit_unsafe_count") or expected_summary.get("pit_warning_count"):
        expected_status = "PASS_WITH_WARNINGS"
    expected_manifest_fields = {
        "expansion_id": expansion_id,
        "start": snapshot.get("start"),
        "end": snapshot.get("end"),
        "generated_at": snapshot.get("generated_at"),
        "status": expected_status,
        "market_regime": "ai_after_chatgpt",
        "data_quality_status": snapshot.get("data_quality_status"),
        "policy_id": _mapping(snapshot.get("policy")).get("policy_id"),
        "policy_version": _mapping(snapshot.get("policy")).get("version"),
        "new_replay_event_count": len(expected_rows),
        "pit_safe_count": expected_summary.get("pit_safe_count"),
        "pit_warning_count": expected_summary.get("pit_warning_count"),
        "pit_unsafe_count": expected_summary.get("pit_unsafe_count"),
        "eligible_count": expected_summary.get("eligible_count"),
        "partial_count": expected_summary.get("partial_count"),
        "ineligible_count": expected_summary.get("ineligible_count"),
    }
    expected_report = render_replay_sample_expansion_report(
        manifest,
        expected_summary,
        expected_inventory,
    )
    source_bundles_match = all(
        _source_bundle_matches(bundle)
        for bundle in _records(snapshot.get("daily_sources"))
        + _records(snapshot.get("replay_inventory_sources"))
        + _records(snapshot.get("owner_review_sources"))
    )
    prices_path = Path(_text(snapshot.get("prices_path")))
    rates_path = Path(_text(snapshot.get("rates_path")))
    policy_path = Path(_text(snapshot.get("policy_path")))
    input_checksums_match = (
        _file_sha256(prices_path) == snapshot.get("prices_checksum")
        and _file_sha256(rates_path) == snapshot.get("rates_checksum")
        and _file_sha256(policy_path) == snapshot.get("policy_checksum")
    )
    try:
        policy_snapshot_matches = _load_replay_sample_expansion_policy(policy_path) == snapshot.get(
            "policy"
        )
        quality = _cached_data_quality_result(
            as_of=_date_from_any(snapshot.get("end")) or date.min,
            prices_path=prices_path,
            rates_path=rates_path,
            enforce=snapshot.get("data_quality_status") != "SKIPPED_EXPLICIT_TEST_FIXTURE",
        )
        quality_status_matches = quality is None or quality.status == snapshot.get(
            "data_quality_status"
        )
        live_source_validation_passes = _replay_expansion_live_source_validation(snapshot)
    except Exception:  # noqa: BLE001
        policy_snapshot_matches = False
        quality_status_matches = False
        live_source_validation_passes = False
    quality_report_path = Path(_text(snapshot.get("data_quality_report_path")))
    data_quality_evidence_matches = not _text(snapshot.get("data_quality_report_path")) or (
        quality_report_path.is_file()
        and _file_sha256(quality_report_path) == snapshot.get("data_quality_report_checksum")
    )
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
            "source_snapshot_schema",
            snapshot.get("schema_version") == REPLAY_SAMPLE_EXPANSION_SNAPSHOT_SCHEMA_VERSION,
            "v2 source snapshot",
        ),
        _check(
            "source_snapshot_checksum_matches",
            manifest.get("source_snapshot_checksum") == _file_sha256(snapshot_path),
            "source snapshot",
        ),
        _check(
            "due_source_selection_explicit",
            snapshot.get("due_source_selection_status")
            in {None, "INCLUDED", "EXCLUDED_POST_COMMITTED_UPDATE"}
            and not (
                snapshot.get("due_source_selection_status")
                == "EXCLUDED_POST_COMMITTED_UPDATE"
                and _records(snapshot.get("due_sources"))
            ),
            _text(snapshot.get("due_source_selection_status"), "LEGACY_INCLUDED"),
        ),
        _check("source_files_unchanged", source_bundles_match, "validated source bundles"),
        _check(
            "live_source_validation_passes",
            live_source_validation_passes,
            "daily/owner/replay validators",
        ),
        _check("cached_and_policy_inputs_unchanged", input_checksums_match, "input checksums"),
        _check("policy_snapshot_matches", policy_snapshot_matches, "reviewed policy"),
        _check("data_quality_status_matches", quality_status_matches, "cached DQ rerun"),
        _check(
            "data_quality_evidence_matches",
            data_quality_evidence_matches,
            "data quality evidence",
        ),
        _check("source_inventory_recomputed", inventory == expected_inventory, recompute_error),
        _check("expanded_events_recomputed", rows == expected_rows, recompute_error),
        _check("pit_summary_recomputed", summary == expected_summary, recompute_error),
        _check(
            "manifest_derived_fields_match",
            all(manifest.get(key) == value for key, value in expected_manifest_fields.items()),
            recompute_error,
        ),
        _check(
            "report_recomputed",
            (expansion_dir / "replay_sample_expansion_report.md").is_file()
            and (expansion_dir / "replay_sample_expansion_report.md").read_text(encoding="utf-8")
            == expected_report,
            "Markdown report",
        ),
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
    policy_path: Path = DEFAULT_OUTCOME_DASHBOARD_POLICY_PATH,
    include_outcome_due_sources: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _aware_utc(generated_at or datetime.now(UTC), "generated_at")
    policy = _load_outcome_dashboard_policy(policy_path)
    snapshot = _build_outcome_dashboard_snapshot(
        generated=generated,
        advisory_outcome_dir=advisory_outcome_dir,
        backfill_dir=backfill_dir,
        repair_dir=repair_dir,
        paper_sim_dir=paper_sim_dir,
        diagnosis_dir=diagnosis_dir,
        outcome_due_dir=outcome_due_dir,
        include_outcome_due_sources=include_outcome_due_sources,
        policy_path=policy_path,
        policy=policy,
    )
    rows_by_mode, selected_pending_sources = _outcome_dashboard_rows_from_snapshot(snapshot)
    matrix = _outcome_availability_matrix(rows_by_mode)
    mode_summary = _outcome_mode_summary(rows_by_mode)
    pending_dashboard = _pending_reason_dashboard_from_rows(
        rows_by_mode=rows_by_mode,
        selected_sources=selected_pending_sources,
        policy=policy,
    )
    reader_brief = _outcome_dashboard_reader_brief(matrix, pending_dashboard)
    dashboard_id = _stable_id("outcome-dashboard", generated.isoformat())
    dashboard_dir = _unique_dir(output_dir / dashboard_id)
    dashboard_dir.mkdir(parents=True, exist_ok=False)
    snapshot["dashboard_id"] = dashboard_dir.name
    snapshot_path = dashboard_dir / "outcome_dashboard_source_snapshot.json"
    _write_json(snapshot_path, snapshot)
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
        "market_regime": "ai_after_chatgpt",
        "source_snapshot_path": str(snapshot_path),
        "source_snapshot_checksum": _file_sha256(snapshot_path),
        "policy_id": policy["policy_id"],
        "policy_version": policy["version"],
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
        "source_snapshot_artifact_path": str(snapshot_path),
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
        "source_snapshot": snapshot,
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
    mode_summary = _read_optional_json(dashboard_dir / "outcome_mode_summary.json") or {}
    pending = _read_optional_json(dashboard_dir / "pending_reason_dashboard.json") or {}
    snapshot_path = dashboard_dir / "outcome_dashboard_source_snapshot.json"
    if not snapshot_path.is_file():
        legacy_checks = [
            _check(
                "manifest_exists",
                (dashboard_dir / "outcome_dashboard_manifest.json").is_file(),
                dashboard_id,
            ),
            _check(
                "matrix_exists",
                (dashboard_dir / "outcome_availability_matrix.json").is_file(),
                dashboard_id,
            ),
            _check(
                "required_modes_present",
                set(_mapping(matrix.get("summary")))
                == {"forward_outcome", "historical_replay", "backtest_simulation"},
                "legacy shallow validation",
            ),
        ]
        payload = _validation_payload(
            report_type="etf_dynamic_v3_outcome_dashboard_validation",
            artifact_id_key="dashboard_id",
            artifact_id=dashboard_id,
            checks=legacy_checks,
        )
        if payload["status"] == "PASS":
            payload["status"] = "PASS_WITH_WARNINGS"
        payload["source_snapshot_status"] = "LEGACY_UNSNAPSHOTTED"
        return payload
    snapshot = _read_optional_json(snapshot_path) or {}
    recompute_error = ""
    try:
        expected_rows_by_mode, selected_pending_sources = (
            _outcome_dashboard_rows_from_snapshot(snapshot)
        )
        expected_matrix = _outcome_availability_matrix(expected_rows_by_mode)
        expected_mode_summary = _outcome_mode_summary(expected_rows_by_mode)
        expected_pending = _pending_reason_dashboard_from_rows(
            rows_by_mode=expected_rows_by_mode,
            selected_sources=selected_pending_sources,
            policy=_mapping(snapshot.get("policy")),
        )
        expected_reader_brief = _outcome_dashboard_reader_brief(
            expected_matrix, expected_pending
        )
    except Exception as exc:  # noqa: BLE001
        recompute_error = str(exc)
        expected_matrix = {}
        expected_mode_summary = {}
        expected_pending = {}
        expected_reader_brief = {}
    total_available = sum(
        _int(item.get("available"))
        for item in _mapping(expected_matrix.get("summary")).values()
    )
    total_pending = sum(
        _int(item.get("pending"))
        for item in _mapping(expected_matrix.get("summary")).values()
    )
    expected_status = "PASS" if total_available else "INSUFFICIENT_DATA"
    if total_pending:
        expected_status = "PASS_WITH_WARNINGS" if total_available else "PENDING"
    expected_manifest_fields = {
        "dashboard_id": dashboard_id,
        "generated_at": snapshot.get("generated_at"),
        "status": expected_status,
        "market_regime": "ai_after_chatgpt",
        "policy_id": _mapping(snapshot.get("policy")).get("policy_id"),
        "policy_version": _mapping(snapshot.get("policy")).get("version"),
        "available_count": total_available,
        "pending_count": total_pending,
        "insufficient_data_count": sum(
            _int(item.get("insufficient_data"))
            for item in _mapping(expected_matrix.get("summary")).values()
        ),
    }
    expected_report = render_outcome_dashboard_report(
        manifest, expected_matrix, expected_pending
    )
    expected_reader_brief_text = render_reader_brief_section(expected_reader_brief)
    bundles = (
        _records(snapshot.get("forward_sources"))
        + _records(snapshot.get("historical_sources"))
        + _records(snapshot.get("simulation_sources"))
        + _records(snapshot.get("diagnosis_sources"))
        + _records(snapshot.get("due_sources"))
    )
    source_bundles_match = all(_source_bundle_matches(bundle) for bundle in bundles)
    policy_path = Path(_text(snapshot.get("policy_path")))
    try:
        policy_matches = (
            _file_sha256(policy_path) == snapshot.get("policy_checksum")
            and _load_outcome_dashboard_policy(policy_path) == snapshot.get("policy")
        )
        live_source_validation_passes = _outcome_dashboard_live_source_validation(snapshot)
    except Exception:  # noqa: BLE001
        policy_matches = False
        live_source_validation_passes = False
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
            "source_snapshot_schema",
            snapshot.get("schema_version") == OUTCOME_DASHBOARD_SNAPSHOT_SCHEMA_VERSION,
            "v2 source snapshot",
        ),
        _check(
            "source_snapshot_checksum_matches",
            manifest.get("source_snapshot_checksum") == _file_sha256(snapshot_path),
            "source snapshot",
        ),
        _check("source_files_unchanged", source_bundles_match, "selected source bundles"),
        _check(
            "live_source_validation_passes",
            live_source_validation_passes,
            "outcome/backfill/sim/diagnosis/due validators",
        ),
        _check("policy_snapshot_matches", policy_matches, "reviewed dashboard policy"),
        _check("matrix_recomputed", matrix == expected_matrix, recompute_error),
        _check(
            "mode_summary_recomputed",
            mode_summary == expected_mode_summary,
            recompute_error,
        ),
        _check("pending_dashboard_recomputed", pending == expected_pending, recompute_error),
        _check(
            "manifest_derived_fields_match",
            all(manifest.get(key) == value for key, value in expected_manifest_fields.items()),
            recompute_error,
        ),
        _check(
            "report_recomputed",
            (dashboard_dir / "outcome_dashboard_report.md").is_file()
            and (dashboard_dir / "outcome_dashboard_report.md").read_text(encoding="utf-8")
            == expected_report,
            "Markdown report",
        ),
        _check(
            "reader_brief_recomputed",
            (dashboard_dir / "reader_brief_section.md").is_file()
            and (dashboard_dir / "reader_brief_section.md").read_text(encoding="utf-8")
            == expected_reader_brief_text,
            "Reader Brief projection",
        ),
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
    policy_path: Path = DEFAULT_LIMITED_VS_NOTRADE_POLICY_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _aware_utc(generated_at or datetime.now(UTC), "generated_at")
    policy = _load_limited_vs_notrade_policy(policy_path)
    snapshot = _build_limited_vs_notrade_snapshot(
        generated=generated,
        advisory_outcome_dir=advisory_outcome_dir,
        backfill_dir=backfill_dir,
        repair_dir=repair_dir,
        policy_path=policy_path,
        policy=policy,
    )
    samples, coverage = _limited_vs_notrade_views_from_snapshot(snapshot)
    metrics = _limited_window_metrics_v2(samples, policy)
    regime_breakdown = _limited_regime_breakdown_v2(samples, policy)
    focus_id = _stable_id("limited-vs-notrade", generated.isoformat())
    focus_dir = _unique_dir(output_dir / focus_id)
    focus_dir.mkdir(parents=True, exist_ok=False)
    available_count = sum(1 for row in samples if row.get("sample_status") == "AVAILABLE")
    recommendation = _limited_overall_recommendation_v2(metrics, policy)
    status = "PASS" if available_count else "INSUFFICIENT_DATA"
    if recommendation == "continue_tracking":
        status = "PASS_WITH_WARNINGS"
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_limited_vs_notrade_manifest",
        "focus_id": focus_dir.name,
        "generated_at": generated.isoformat(),
        "status": status,
        "source_snapshot_path": str(focus_dir / "limited_vs_notrade_source_snapshot.json"),
        "source_snapshot_checksum": "",
        "policy_id": policy["policy_id"],
        "policy_version": policy["version"],
        "available_count": available_count,
        "paired_sample_count": len(samples),
        "unpaired_source_row_count": coverage["unpaired_source_row_count"],
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
        "pair_coverage": coverage,
        "policy_id": policy["policy_id"],
        "policy_version": policy["version"],
        "policy_mutated": False,
        "production_effect": "none",
        "broker_action_taken": False,
    }
    snapshot["focus_id"] = focus_dir.name
    snapshot_path = focus_dir / "limited_vs_notrade_source_snapshot.json"
    _write_json(snapshot_path, snapshot)
    manifest["source_snapshot_checksum"] = _file_sha256(snapshot_path)
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
        "source_snapshot": snapshot,
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
    regime = _read_optional_json(focus_dir / "regime_breakdown.json") or {}
    snapshot_path = focus_dir / "limited_vs_notrade_source_snapshot.json"
    if not snapshot_path.is_file():
        legacy_checks = [
            _check(
                "manifest_exists",
                (focus_dir / "limited_vs_notrade_manifest.json").is_file(),
                focus_id,
            ),
            _check("samples_exists", (focus_dir / "sample_inventory.jsonl").is_file(), focus_id),
            _check(
                "no_auto_policy_apply",
                manifest.get("auto_policy_apply") is False,
                "legacy shallow validation",
            ),
        ]
        payload = _validation_payload(
            report_type="etf_dynamic_v3_limited_vs_notrade_validation",
            artifact_id_key="focus_id",
            artifact_id=focus_id,
            checks=legacy_checks,
        )
        if payload["status"] == "PASS":
            payload["status"] = "PASS_WITH_WARNINGS"
        payload["source_snapshot_status"] = "LEGACY_UNSNAPSHOTTED"
        return payload
    snapshot = _read_optional_json(snapshot_path) or {}
    recompute_error = ""
    try:
        expected_samples, expected_coverage = _limited_vs_notrade_views_from_snapshot(snapshot)
        policy = _mapping(snapshot.get("policy"))
        expected_metric_rows = _limited_window_metrics_v2(expected_samples, policy)
        expected_recommendation = _limited_overall_recommendation_v2(
            expected_metric_rows, policy
        )
        expected_metrics = {
            "schema_version": SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_limited_vs_notrade_window_comparison_metrics",
            "by_window": expected_metric_rows,
            "overall_recommendation": expected_recommendation,
            "pair_coverage": expected_coverage,
            "policy_id": policy.get("policy_id"),
            "policy_version": policy.get("version"),
            "policy_mutated": False,
            "production_effect": "none",
            "broker_action_taken": False,
        }
        expected_regime = _limited_regime_breakdown_v2(expected_samples, policy)
    except Exception as exc:  # noqa: BLE001
        recompute_error = str(exc)
        expected_samples, expected_metrics, expected_regime = [], {}, {}
        expected_recommendation = "insufficient_data"
        expected_coverage = {}
    available_count = sum(
        1 for row in expected_samples if row.get("sample_status") == "AVAILABLE"
    )
    expected_status = "PASS" if available_count else "INSUFFICIENT_DATA"
    if expected_recommendation == "continue_tracking":
        expected_status = "PASS_WITH_WARNINGS"
    expected_manifest_fields = {
        "focus_id": focus_id,
        "generated_at": snapshot.get("generated_at"),
        "status": expected_status,
        "policy_id": _mapping(snapshot.get("policy")).get("policy_id"),
        "policy_version": _mapping(snapshot.get("policy")).get("version"),
        "available_count": available_count,
        "paired_sample_count": len(expected_samples),
        "unpaired_source_row_count": expected_coverage.get("unpaired_source_row_count"),
        "overall_recommendation": expected_recommendation,
    }
    expected_report = render_limited_vs_notrade_report(
        manifest, expected_metrics, expected_regime
    )
    bundles = _records(snapshot.get("forward_sources")) + _records(
        snapshot.get("historical_sources")
    )
    source_bundles_match = all(_source_bundle_matches(bundle) for bundle in bundles)
    policy_path = Path(_text(snapshot.get("policy_path")))
    try:
        policy_matches = (
            _file_sha256(policy_path) == snapshot.get("policy_checksum")
            and _load_limited_vs_notrade_policy(policy_path) == snapshot.get("policy")
        )
        live_source_validation_passes = _limited_vs_notrade_live_source_validation(snapshot)
    except Exception:  # noqa: BLE001
        policy_matches = False
        live_source_validation_passes = False
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
            "source_snapshot_schema",
            snapshot.get("schema_version") == LIMITED_VS_NOTRADE_SNAPSHOT_SCHEMA_VERSION,
            "v2 source snapshot",
        ),
        _check(
            "source_snapshot_checksum_matches",
            manifest.get("source_snapshot_checksum") == _file_sha256(snapshot_path),
            "source snapshot",
        ),
        _check("source_files_unchanged", source_bundles_match, "selected source bundles"),
        _check(
            "live_source_validation_passes",
            live_source_validation_passes,
            "advisory outcome / repair / backfill validators",
        ),
        _check("policy_snapshot_matches", policy_matches, "reviewed comparison policy"),
        _check("samples_recomputed", samples == expected_samples, recompute_error),
        _check("metrics_recomputed", metrics == expected_metrics, recompute_error),
        _check("regime_recomputed", regime == expected_regime, recompute_error),
        _check(
            "manifest_derived_fields_match",
            all(manifest.get(key) == value for key, value in expected_manifest_fields.items()),
            recompute_error,
        ),
        _check(
            "report_recomputed",
            (focus_dir / "limited_vs_notrade_report.md").is_file()
            and (focus_dir / "limited_vs_notrade_report.md").read_text(encoding="utf-8")
            == expected_report,
            "Markdown report",
        ),
        _check(
            "sample_status_valid",
            all(row.get("sample_status") in OUTCOME_WINDOW_STATUSES for row in samples),
            "sample status",
        ),
        _check(
            "insufficient_data_explicit",
            all(
                row.get("confidence") == "INSUFFICIENT_DATA"
                and row.get("avg_relative_return") is None
                and row.get("win_rate") is None
                for row in _records(metrics.get("by_window"))
                if _int(row.get("available_count")) == 0
            ),
            "empty metrics must be null and insufficient",
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
    policy_path: Path = DEFAULT_CONSENSUS_RISK_POLICY_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _aware_utc(generated_at or datetime.now(UTC), "generated_at")
    policy = _load_consensus_risk_policy(policy_path)
    snapshot = _build_consensus_risk_snapshot(
        generated=generated,
        daily_advisory_dir=daily_advisory_dir,
        historical_replay_dir=historical_replay_dir,
        backfill_dir=backfill_dir,
        repair_dir=repair_dir,
        policy_path=policy_path,
        policy=policy,
    )
    exposure_samples, coverage, outcome_pairs = _consensus_risk_views_from_snapshot(snapshot)
    exposure = _consensus_exposure_summary_v2(exposure_samples, policy, coverage)
    drawdown = _consensus_drawdown_risk_v2(outcome_pairs, policy)
    turnover = _consensus_turnover_risk_v2(exposure_samples, policy)
    risk_status = _consensus_overall_risk_status_v2(exposure, drawdown, turnover, policy)
    risk_id = _stable_id("consensus-risk", generated.isoformat())
    risk_dir = _unique_dir(output_dir / risk_id)
    risk_dir.mkdir(parents=True, exist_ok=False)
    snapshot["risk_id"] = risk_dir.name
    snapshot_path = risk_dir / "consensus_risk_source_snapshot.json"
    _write_json(snapshot_path, snapshot)
    manifest = _consensus_risk_manifest(
        risk_dir=risk_dir,
        generated_at=generated.isoformat(),
        risk_status=risk_status,
        exposure=exposure,
        drawdown=drawdown,
        coverage=coverage,
        policy=policy,
        source_snapshot_checksum=_file_sha256(snapshot_path),
    )
    _write_json(risk_dir / "consensus_risk_manifest.json", manifest)
    _write_jsonl(risk_dir / "consensus_exposure_samples.jsonl", exposure_samples)
    _write_jsonl(risk_dir / "consensus_drawdown_pairs.jsonl", outcome_pairs)
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
        "consensus_exposure_samples": exposure_samples,
        "consensus_drawdown_pairs": outcome_pairs,
        "consensus_exposure_summary": exposure,
        "consensus_drawdown_risk": drawdown,
        "consensus_turnover_risk": turnover,
        "source_snapshot": snapshot,
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
    drawdown = _read_optional_json(risk_dir / "consensus_drawdown_risk.json") or {}
    turnover = _read_optional_json(risk_dir / "consensus_turnover_risk.json") or {}
    exposure_samples = _read_jsonl(risk_dir / "consensus_exposure_samples.jsonl")
    drawdown_pairs = _read_jsonl(risk_dir / "consensus_drawdown_pairs.jsonl")
    snapshot_path = risk_dir / "consensus_risk_source_snapshot.json"
    if not snapshot_path.is_file():
        legacy_checks = [
            _check(
                "manifest_exists",
                (risk_dir / "consensus_risk_manifest.json").is_file(),
                risk_id,
            ),
            _check(
                "no_default_consensus_execution",
                manifest.get("consensus_target_default_execution_recommended") is False
                and manifest.get("auto_policy_apply") is False,
                "legacy shallow validation",
            ),
        ]
        payload = _validation_payload(
            report_type="etf_dynamic_v3_consensus_risk_validation",
            artifact_id_key="risk_id",
            artifact_id=risk_id,
            checks=legacy_checks,
        )
        if payload["status"] == "PASS":
            payload["status"] = "PASS_WITH_WARNINGS"
        payload["source_snapshot_status"] = "LEGACY_UNSNAPSHOTTED"
        return payload
    snapshot = _read_optional_json(snapshot_path) or {}
    recompute_error = ""
    try:
        expected_samples, expected_coverage, expected_pairs = (
            _consensus_risk_views_from_snapshot(snapshot)
        )
        policy = _mapping(snapshot.get("policy"))
        expected_exposure = _consensus_exposure_summary_v2(
            expected_samples, policy, expected_coverage
        )
        expected_drawdown = _consensus_drawdown_risk_v2(expected_pairs, policy)
        expected_turnover = _consensus_turnover_risk_v2(expected_samples, policy)
        expected_status = _consensus_overall_risk_status_v2(
            expected_exposure, expected_drawdown, expected_turnover, policy
        )
        expected_manifest = _consensus_risk_manifest(
            risk_dir=risk_dir,
            generated_at=_text(snapshot.get("generated_at")),
            risk_status=expected_status,
            exposure=expected_exposure,
            drawdown=expected_drawdown,
            coverage=expected_coverage,
            policy=policy,
            source_snapshot_checksum=_file_sha256(snapshot_path),
        )
        expected_report = render_consensus_risk_report(
            expected_manifest,
            expected_exposure,
            expected_drawdown,
            expected_turnover,
        )
    except Exception as exc:  # noqa: BLE001
        recompute_error = str(exc)
        expected_samples, expected_pairs = [], []
        expected_exposure, expected_drawdown, expected_turnover = {}, {}, {}
        expected_manifest, expected_report = {}, ""
    bundles = (
        _records(snapshot.get("daily_sources"))
        + _records(snapshot.get("historical_replay_sources"))
        + _records(snapshot.get("outcome_sources"))
    )
    source_bundles_match = all(_source_bundle_matches(bundle) for bundle in bundles)
    policy_path = Path(_text(snapshot.get("policy_path")))
    try:
        policy_matches = (
            _file_sha256(policy_path) == snapshot.get("policy_checksum")
            and _load_consensus_risk_policy(policy_path) == snapshot.get("policy")
        )
        live_source_validation_passes = _consensus_risk_live_source_validation(snapshot)
    except Exception:  # noqa: BLE001
        policy_matches = False
        live_source_validation_passes = False
    checks = [
        _check("manifest_exists", (risk_dir / "consensus_risk_manifest.json").exists(), risk_id),
        _check(
            "exposure_exists",
            (risk_dir / "consensus_exposure_summary.json").exists(),
            risk_id,
        ),
        _check(
            "exposure_samples_exist",
            (risk_dir / "consensus_exposure_samples.jsonl").exists(),
            risk_id,
        ),
        _check(
            "drawdown_pairs_exist",
            (risk_dir / "consensus_drawdown_pairs.jsonl").exists(),
            risk_id,
        ),
        _check("drawdown_exists", (risk_dir / "consensus_drawdown_risk.json").exists(), risk_id),
        _check("turnover_exists", (risk_dir / "consensus_turnover_risk.json").exists(), risk_id),
        _check("report_exists", (risk_dir / "consensus_risk_report.md").exists(), risk_id),
        _check("risk_id_matches", manifest.get("risk_id") == risk_id, risk_id),
        _check(
            "source_snapshot_schema",
            snapshot.get("schema_version") == CONSENSUS_RISK_SNAPSHOT_SCHEMA_VERSION,
            "v2 source snapshot",
        ),
        _check(
            "source_snapshot_checksum_matches",
            manifest.get("source_snapshot_checksum") == _file_sha256(snapshot_path),
            "source snapshot",
        ),
        _check("source_files_unchanged", source_bundles_match, "selected source bundles"),
        _check(
            "live_source_validation_passes",
            live_source_validation_passes,
            "daily/replay/outcome validators",
        ),
        _check("policy_snapshot_matches", policy_matches, "reviewed risk policy"),
        _check(
            "exposure_samples_recomputed",
            exposure_samples == expected_samples,
            recompute_error,
        ),
        _check("drawdown_pairs_recomputed", drawdown_pairs == expected_pairs, recompute_error),
        _check("exposure_recomputed", exposure == expected_exposure, recompute_error),
        _check("drawdown_recomputed", drawdown == expected_drawdown, recompute_error),
        _check("turnover_recomputed", turnover == expected_turnover, recompute_error),
        _check("manifest_recomputed", manifest == expected_manifest, recompute_error),
        _check(
            "report_recomputed",
            (risk_dir / "consensus_risk_report.md").is_file()
            and (risk_dir / "consensus_risk_report.md").read_text(encoding="utf-8")
            == expected_report,
            recompute_error,
        ),
        _check(
            "missing_metrics_are_null",
            all(
                row.get("avg_drawdown") is not None
                or (
                    row.get("max_drawdown") is None
                    and row.get("drawdown_delta_vs_no_trade") is None
                )
                for row in _records(drawdown.get("window_results"))
            )
            and (
                turnover.get("avg_turnover") is not None
                or turnover.get("max_turnover") is None
            ),
            "missing risk metrics must remain null",
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


def run_outcome_update_review(
    *,
    due_id: str,
    output_dir: Path = DEFAULT_OUTCOME_UPDATE_REVIEW_DIR,
    outcome_due_dir: Path = DEFAULT_OUTCOME_DUE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _aware_utc(generated_at or datetime.now(UTC), "generated_at")
    snapshot = _build_outcome_update_review_snapshot(
        due_id=due_id, outcome_due_dir=outcome_due_dir, generated=generated
    )
    review_rows, safety_checks, impact_preview, status = _outcome_update_review_views(
        snapshot
    )
    review_id = _stable_id("outcome-update-review", due_id, generated.isoformat())
    review_dir = _unique_dir(output_dir / review_id)
    review_dir.mkdir(parents=True, exist_ok=False)
    snapshot["update_review_id"] = review_dir.name
    snapshot_path = review_dir / "outcome_update_review_source_snapshot.json"
    _write_json(snapshot_path, snapshot)
    manifest = _outcome_update_review_manifest(
        review_dir=review_dir,
        due_id=due_id,
        as_of=_text(snapshot.get("as_of")),
        generated_at=generated.isoformat(),
        status=status,
        rows=review_rows,
        safety=safety_checks,
        source_snapshot_checksum=_file_sha256(snapshot_path),
    )
    _write_json(review_dir / "outcome_update_review_manifest.json", manifest)
    _write_jsonl(review_dir / "update_ready_review_matrix.jsonl", review_rows)
    _write_json(review_dir / "update_impact_preview.json", impact_preview)
    _write_json(review_dir / "update_safety_checks.json", safety_checks)
    _write_text(
        review_dir / "outcome_update_review_report.md",
        render_outcome_update_review_report(manifest, safety_checks, impact_preview),
    )
    _update_latest_pointer(
        "latest_outcome_update_review",
        review_dir.name,
        review_dir / "outcome_update_review_manifest.json",
    )
    return {
        "update_review_id": review_dir.name,
        "review_dir": review_dir,
        "manifest": manifest,
        "update_ready_review_matrix": review_rows,
        "update_impact_preview": impact_preview,
        "update_safety_checks": safety_checks,
        "source_snapshot": snapshot,
    }


def outcome_update_review_report_payload(
    *,
    review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_OUTCOME_UPDATE_REVIEW_DIR,
) -> dict[str, Any]:
    review_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=review_id if not latest else None,
        pointer_name="latest_outcome_update_review",
    )
    return {
        **_read_json(review_dir / "outcome_update_review_manifest.json"),
        "update_ready_review_matrix": _read_jsonl(review_dir / "update_ready_review_matrix.jsonl"),
        "update_impact_preview": _read_json(review_dir / "update_impact_preview.json"),
        "update_safety_checks": _read_json(review_dir / "update_safety_checks.json"),
        "review_dir": str(review_dir),
    }


def validate_outcome_update_review_artifact(
    *, review_id: str, output_dir: Path = DEFAULT_OUTCOME_UPDATE_REVIEW_DIR
) -> dict[str, Any]:
    review_dir = output_dir / review_id
    manifest = _read_optional_json(review_dir / "outcome_update_review_manifest.json") or {}
    rows = _read_jsonl(review_dir / "update_ready_review_matrix.jsonl")
    safety = _read_optional_json(review_dir / "update_safety_checks.json") or {}
    impact = _read_optional_json(review_dir / "update_impact_preview.json") or {}
    snapshot_path = review_dir / "outcome_update_review_source_snapshot.json"
    if not snapshot_path.is_file():
        legacy_checks = [
            _check(
                "manifest_exists",
                (review_dir / "outcome_update_review_manifest.json").is_file(),
                review_id,
            ),
            _check(
                "broker_action_forbidden",
                manifest.get("broker_action_allowed") is False
                and manifest.get("broker_action_taken") is False,
                "legacy shallow validation",
            ),
        ]
        payload = _validation_payload(
            report_type="etf_dynamic_v3_outcome_update_review_validation",
            artifact_id_key="review_id",
            artifact_id=review_id,
            checks=legacy_checks,
        )
        if payload["status"] == "PASS":
            payload["status"] = "PASS_WITH_WARNINGS"
        payload["source_snapshot_status"] = "LEGACY_UNSNAPSHOTTED"
        return payload
    snapshot = _read_optional_json(snapshot_path) or {}
    recompute_error = ""
    try:
        expected_rows, expected_safety, expected_impact, expected_status = (
            _outcome_update_review_views(snapshot)
        )
        expected_manifest = _outcome_update_review_manifest(
            review_dir=review_dir,
            due_id=_text(snapshot.get("due_id")),
            as_of=_text(snapshot.get("as_of")),
            generated_at=_text(snapshot.get("generated_at")),
            status=expected_status,
            rows=expected_rows,
            safety=expected_safety,
            source_snapshot_checksum=_file_sha256(snapshot_path),
        )
        expected_report = render_outcome_update_review_report(
            expected_manifest, expected_safety, expected_impact
        )
    except Exception as exc:  # noqa: BLE001
        recompute_error = str(exc)
        expected_rows, expected_safety, expected_impact = [], {}, {}
        expected_manifest, expected_report = {}, ""
    bundle = _mapping(snapshot.get("due_source_bundle"))
    source_matches = _source_bundle_matches(bundle)
    try:
        source_validation_passes = (
            validate_outcome_due_artifact(
                due_id=_text(snapshot.get("due_id")),
                output_dir=Path(_text(snapshot.get("outcome_due_dir"))),
            ).get("status")
            == "PASS"
        )
    except Exception:  # noqa: BLE001
        source_validation_passes = False
    checks = [
        _check(
            "manifest_exists",
            (review_dir / "outcome_update_review_manifest.json").exists(),
            review_id,
        ),
        _check(
            "review_matrix_exists",
            (review_dir / "update_ready_review_matrix.jsonl").exists(),
            review_id,
        ),
        _check(
            "impact_preview_exists",
            (review_dir / "update_impact_preview.json").exists(),
            review_id,
        ),
        _check(
            "safety_checks_exists",
            (review_dir / "update_safety_checks.json").exists(),
            review_id,
        ),
        _check(
            "report_exists",
            (review_dir / "outcome_update_review_report.md").exists(),
            review_id,
        ),
        _check("review_id_matches", manifest.get("update_review_id") == review_id, review_id),
        _check(
            "source_snapshot_schema",
            snapshot.get("schema_version") == OUTCOME_UPDATE_REVIEW_SNAPSHOT_SCHEMA_VERSION,
            "v2 source snapshot",
        ),
        _check(
            "source_snapshot_checksum_matches",
            manifest.get("source_snapshot_checksum") == _file_sha256(snapshot_path),
            "source snapshot",
        ),
        _check("source_files_unchanged", source_matches, "outcome due source"),
        _check("source_validation_passes", source_validation_passes, "outcome due validator"),
        _check("review_rows_recomputed", rows == expected_rows, recompute_error),
        _check("safety_recomputed", safety == expected_safety, recompute_error),
        _check("impact_recomputed", impact == expected_impact, recompute_error),
        _check("manifest_recomputed", manifest == expected_manifest, recompute_error),
        _check(
            "report_recomputed",
            (review_dir / "outcome_update_review_report.md").is_file()
            and (review_dir / "outcome_update_review_report.md").read_text(encoding="utf-8")
            == expected_report,
            recompute_error,
        ),
        _check(
            "future_data_not_used_in_decision",
            all(row.get("future_data_used_in_decision") is False for row in rows)
            and safety.get("future_data_used_in_decision") is False,
            "future data decision leakage forbidden",
        ),
        _check(
            "ready_rows_are_due_and_pending",
            all(
                row.get("due_status") == "DUE"
                and row.get("existing_status") == "PENDING"
                and row.get("can_update") is True
                for row in rows
                if row.get("review_status") == "READY_TO_UPDATE"
            ),
            "ready rows",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action forbidden",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_outcome_update_review_validation",
        artifact_id_key="review_id",
        artifact_id=review_id,
        checks=checks,
    )


def run_outcome_update(
    *,
    update_review_id: str,
    output_dir: Path = DEFAULT_OUTCOME_UPDATE_DIR,
    review_dir: Path = DEFAULT_OUTCOME_UPDATE_REVIEW_DIR,
    advisory_outcome_dir: Path = DEFAULT_ADVISORY_OUTCOME_DIR,
    paper_portfolio_dir: Path | None = None,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _aware_utc(generated_at or datetime.now(UTC), "generated_at")
    _safe_child_dir(review_dir, update_review_id, "update_review_id")
    _recover_prepared_outcome_updates(
        output_dir=output_dir,
        update_review_id=update_review_id,
        advisory_outcome_dir=advisory_outcome_dir,
    )
    if _committed_outcome_update_for_review(output_dir, update_review_id):
        raise DynamicV3OutcomeAccumulationError(
            "outcome update review already has a COMMITTED update"
        )
    review_validation = validate_outcome_update_review_artifact(
        review_id=update_review_id,
        output_dir=review_dir,
    )
    if review_validation.get("status") != "PASS":
        raise DynamicV3OutcomeAccumulationError(
            "outcome update review validation must PASS before mutation; "
            f"status={review_validation.get('status')}"
        )
    review_artifact_dir = review_dir / update_review_id
    review_manifest = _read_json(review_artifact_dir / "outcome_update_review_manifest.json")
    review_generated = _datetime_from_any(review_manifest.get("generated_at"))
    as_of = _date_from_any(review_manifest.get("as_of"))
    if review_generated is None or as_of is None:
        raise DynamicV3OutcomeAccumulationError(
            "outcome update review missing timezone-aware generated_at or as_of"
        )
    if generated < review_generated or as_of > generated.date():
        raise DynamicV3OutcomeAccumulationError(
            "outcome update generated_at must not precede review or as_of"
        )
    review_rows = _read_jsonl(review_artifact_dir / "update_ready_review_matrix.jsonl")
    identities = [
        (_text(row.get("outcome_id")), _int(row.get("window_days")))
        for row in review_rows
    ]
    if (
        len(identities) != len(set(identities))
        or any(not outcome_id or window_days <= 0 for outcome_id, window_days in identities)
    ):
        raise DynamicV3OutcomeAccumulationError(
            "outcome update review rows require unique non-empty outcome-window identities"
        )
    ready_rows = [row for row in review_rows if row.get("review_status") == "READY_TO_UPDATE"]
    ready_outcome_ids = sorted({_text(row.get("outcome_id")) for row in ready_rows})
    ready_keys = set(identities) & {
        (_text(row.get("outcome_id")), _int(row.get("window_days"))) for row in ready_rows
    }
    selected_outcome_ids = sorted({_text(row.get("outcome_id")) for row in review_rows})
    pre_bundles, pre_validations = _validated_outcome_update_bundles(
        outcome_ids=selected_outcome_ids,
        advisory_outcome_dir=advisory_outcome_dir,
    )
    before_by_window = _outcome_rows_by_window_from_bundles(pre_bundles)
    for row in review_rows:
        key = (_text(row.get("outcome_id")), _int(row.get("window_days")))
        before = before_by_window.get(key)
        if before is None or before.get("outcome_status") != row.get("existing_status"):
            raise DynamicV3OutcomeAccumulationError(
                f"outcome update live pre-state drift for {key[0]}:{key[1]}"
            )
    resolved_paper_dir = (
        DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_portfolio"
        if paper_portfolio_dir is None
        else paper_portfolio_dir
    )
    _preflight_outcome_update_batch(
        outcome_ids=ready_outcome_ids,
        ready_keys=ready_keys,
        advisory_outcome_dir=advisory_outcome_dir,
        paper_portfolio_dir=resolved_paper_dir,
        prices_path=prices_path,
        rates_path=rates_path,
        as_of=as_of,
        generated=generated,
    )
    update_id = _stable_id("outcome-update", update_review_id, generated.isoformat())
    update_dir = output_dir / update_id
    if update_dir.exists():
        raise DynamicV3OutcomeAccumulationError(
            f"outcome update transaction already exists: {update_dir}"
        )
    update_dir.mkdir(parents=True, exist_ok=False)
    backup_root = update_dir / "rollback_backups"
    backup_paths = _backup_outcome_update_sources(
        outcome_ids=ready_outcome_ids,
        advisory_outcome_dir=advisory_outcome_dir,
        backup_root=backup_root,
    )
    transaction_path = update_dir / "outcome_update_transaction.json"
    snapshot_path = update_dir / "outcome_update_source_snapshot.json"
    transaction = _outcome_update_transaction(
        update_id=update_id,
        update_review_id=update_review_id,
        generated=generated,
        status="PREPARED",
        selected_outcome_ids=selected_outcome_ids,
        ready_outcome_ids=ready_outcome_ids,
        backup_paths=backup_paths,
    )
    snapshot: dict[str, Any] = {
        "schema_version": OUTCOME_UPDATE_SNAPSHOT_SCHEMA_VERSION,
        "outcome_update_id": update_id,
        "update_review_id": update_review_id,
        "generated_at": generated.isoformat(),
        "review_generated_at": review_generated.isoformat(),
        "as_of": as_of.isoformat(),
        "future_data_used_in_decision": False,
        "selected_outcome_ids": selected_outcome_ids,
        "ready_outcome_ids": ready_outcome_ids,
        "ready_keys": [list(key) for key in sorted(ready_keys)],
        "review_source_bundle": _immutable_source_bundle(review_artifact_dir),
        "review_validation": review_validation,
        "pre_outcome_bundles": pre_bundles,
        "pre_outcome_validations": pre_validations,
        "post_outcome_bundles": {},
        "post_outcome_validations": {},
        "transaction_status": "PREPARED",
        "production_effect": "none",
        "broker_action_taken": False,
    }
    _write_json(transaction_path, transaction)
    _write_json(snapshot_path, snapshot)
    try:
        for outcome_id in ready_outcome_ids:
            update_advisory_outcome(
                as_of=as_of,
                outcome_id=outcome_id,
                output_dir=advisory_outcome_dir,
                paper_portfolio_dir=resolved_paper_dir,
                prices_path=prices_path,
                rates_path=rates_path,
                generated_at=generated,
                allowed_window_days={
                    window_days
                    for selected_outcome_id, window_days in ready_keys
                    if selected_outcome_id == outcome_id
                },
            )
        post_bundles, post_validations = _validated_outcome_update_bundles(
            outcome_ids=selected_outcome_ids,
            advisory_outcome_dir=advisory_outcome_dir,
        )
        snapshot["post_outcome_bundles"] = post_bundles
        snapshot["post_outcome_validations"] = post_validations
        snapshot["transaction_status"] = "COMMITTED"
        updated_windows, skipped_windows, status_delta = _outcome_update_derived_views(snapshot)
        _write_json(snapshot_path, snapshot)
        manifest = _outcome_update_manifest(
            update_dir=update_dir,
            update_review_id=update_review_id,
            review_manifest=review_manifest,
            generated=generated,
            as_of=as_of,
            updated_windows=updated_windows,
            skipped_windows=skipped_windows,
            source_snapshot_checksum=_file_sha256(snapshot_path),
        )
        _write_json(update_dir / "outcome_update_manifest.json", manifest)
        _write_jsonl(update_dir / "updated_windows.jsonl", updated_windows)
        _write_jsonl(update_dir / "skipped_windows.jsonl", skipped_windows)
        _write_json(update_dir / "outcome_status_delta.json", status_delta)
        _write_text(
            update_dir / "outcome_update_report.md",
            render_outcome_update_report(
                manifest, status_delta, updated_windows, skipped_windows
            ),
        )
        if backup_root.exists():
            shutil.rmtree(backup_root)
        transaction = _outcome_update_transaction(
            update_id=update_id,
            update_review_id=update_review_id,
            generated=generated,
            status="COMMITTED",
            selected_outcome_ids=selected_outcome_ids,
            ready_outcome_ids=ready_outcome_ids,
            backup_paths=[],
        )
        _write_json(transaction_path, transaction)
    except Exception as exc:
        rollback_validation = _restore_outcome_update_sources(
            outcome_ids=ready_outcome_ids,
            advisory_outcome_dir=advisory_outcome_dir,
            backup_root=backup_root,
        )
        transaction = _outcome_update_transaction(
            update_id=update_id,
            update_review_id=update_review_id,
            generated=generated,
            status="ROLLED_BACK",
            selected_outcome_ids=selected_outcome_ids,
            ready_outcome_ids=ready_outcome_ids,
            backup_paths=[],
            error=str(exc),
            rollback_validation=rollback_validation,
        )
        _write_json(transaction_path, transaction)
        raise DynamicV3OutcomeAccumulationError(
            f"outcome update transaction rolled back: {exc}"
        ) from exc
    _update_latest_pointer(
        "latest_outcome_update",
        update_dir.name,
        update_dir / "outcome_update_manifest.json",
    )
    return {
        "outcome_update_id": update_dir.name,
        "outcome_update_dir": update_dir,
        "manifest": manifest,
        "updated_windows": updated_windows,
        "skipped_windows": skipped_windows,
        "outcome_status_delta": status_delta,
        "source_snapshot": snapshot,
        "transaction": transaction,
    }


def outcome_update_report_payload(
    *,
    update_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_OUTCOME_UPDATE_DIR,
) -> dict[str, Any]:
    update_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=update_id if not latest else None,
        pointer_name="latest_outcome_update",
    )
    return {
        **_read_json(update_dir / "outcome_update_manifest.json"),
        "source_snapshot": _read_json(update_dir / "outcome_update_source_snapshot.json"),
        "transaction": _read_json(update_dir / "outcome_update_transaction.json"),
        "updated_windows": _read_jsonl(update_dir / "updated_windows.jsonl"),
        "skipped_windows": _read_jsonl(update_dir / "skipped_windows.jsonl"),
        "outcome_status_delta": _read_json(update_dir / "outcome_status_delta.json"),
        "outcome_update_dir": str(update_dir),
    }


def validate_outcome_update_artifact(
    *, update_id: str, output_dir: Path = DEFAULT_OUTCOME_UPDATE_DIR
) -> dict[str, Any]:
    update_dir = output_dir / update_id
    snapshot_path = update_dir / "outcome_update_source_snapshot.json"
    transaction_path = update_dir / "outcome_update_transaction.json"
    manifest = _read_optional_json(update_dir / "outcome_update_manifest.json") or {}
    updated = _read_jsonl(update_dir / "updated_windows.jsonl")
    skipped = _read_jsonl(update_dir / "skipped_windows.jsonl")
    delta = _read_optional_json(update_dir / "outcome_status_delta.json") or {}
    snapshot = _read_optional_json(snapshot_path) or {}
    transaction = _read_optional_json(transaction_path) or {}
    report = _read_text(update_dir / "outcome_update_report.md")
    try:
        expected_updated, expected_skipped, expected_delta = _outcome_update_derived_views(
            snapshot
        )
        review_bundle = _mapping(snapshot.get("review_source_bundle"))
        review_manifest = _mapping(
            _source_bundle_content(review_bundle, "outcome_update_review_manifest.json")
        )
        generated = _datetime_from_any(snapshot.get("generated_at"))
        as_of = _date_from_any(snapshot.get("as_of"))
        expected_manifest = (
            _outcome_update_manifest(
                update_dir=update_dir,
                update_review_id=_text(snapshot.get("update_review_id")),
                review_manifest=review_manifest,
                generated=generated,
                as_of=as_of,
                updated_windows=expected_updated,
                skipped_windows=expected_skipped,
                source_snapshot_checksum=_file_sha256(snapshot_path),
            )
            if generated is not None and as_of is not None
            else {}
        )
        expected_report = render_outcome_update_report(
            expected_manifest,
            expected_delta,
            expected_updated,
            expected_skipped,
        )
        snapshot_errors = _outcome_update_snapshot_errors(snapshot)
    except Exception as exc:  # noqa: BLE001
        expected_updated, expected_skipped, expected_delta = [], [], {}
        expected_manifest, expected_report = {}, ""
        snapshot_errors = [f"snapshot_recompute_error:{exc}"]
    checks = [
        _check(
            "manifest_exists", (update_dir / "outcome_update_manifest.json").exists(), update_id
        ),
        _check(
            "updated_windows_exists",
            (update_dir / "updated_windows.jsonl").exists(),
            update_id,
        ),
        _check(
            "skipped_windows_exists",
            (update_dir / "skipped_windows.jsonl").exists(),
            update_id,
        ),
        _check(
            "status_delta_exists", (update_dir / "outcome_status_delta.json").exists(), update_id
        ),
        _check("report_exists", (update_dir / "outcome_update_report.md").exists(), update_id),
        _check("source_snapshot_exists", snapshot_path.exists(), update_id),
        _check("transaction_exists", transaction_path.exists(), update_id),
        _check("update_id_matches", manifest.get("outcome_update_id") == update_id, update_id),
        _check(
            "source_snapshot_schema",
            snapshot.get("schema_version") == OUTCOME_UPDATE_SNAPSHOT_SCHEMA_VERSION,
            _text(snapshot.get("schema_version")),
        ),
        _check(
            "source_snapshot_checksum_matches",
            manifest.get("source_snapshot_checksum") == _file_sha256(snapshot_path),
            str(snapshot_path),
        ),
        _check(
            "transaction_committed",
            transaction.get("schema_version") == OUTCOME_UPDATE_TRANSACTION_SCHEMA_VERSION
            and transaction.get("status") == "COMMITTED"
            and transaction.get("outcome_update_id") == update_id
            and transaction.get("update_review_id") == manifest.get("update_review_id")
            and transaction.get("rollback_required") is False,
            _text(transaction.get("status")),
        ),
        _check(
            "snapshot_contract_recomputed",
            not snapshot_errors,
            ",".join(snapshot_errors),
        ),
        _check("updated_windows_recomputed", updated == expected_updated, "updated windows"),
        _check("skipped_windows_recomputed", skipped == expected_skipped, "skipped windows"),
        _check("status_delta_recomputed", delta == expected_delta, "status delta"),
        _check("manifest_recomputed", manifest == expected_manifest, "manifest"),
        _check("report_recomputed", report == expected_report, "Markdown"),
        _check(
            "live_post_outcomes_match",
            all(
                _source_bundle_matches(_mapping(bundle))
                for bundle in _mapping(snapshot.get("post_outcome_bundles")).values()
            ),
            "committed outcome bundles",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action forbidden",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_outcome_update_validation",
        artifact_id_key="update_id",
        artifact_id=update_id,
        checks=checks,
    )


def run_rolling_evidence_refresh(
    *,
    outcome_update_id: str,
    output_dir: Path = DEFAULT_ROLLING_EVIDENCE_REFRESH_DIR,
    outcome_update_dir: Path = DEFAULT_OUTCOME_UPDATE_DIR,
    outcome_dashboard_dir: Path = DEFAULT_OUTCOME_DASHBOARD_DIR,
    limited_vs_notrade_dir: Path = DEFAULT_LIMITED_VS_NOTRADE_DIR,
    consensus_risk_dir: Path = DEFAULT_CONSENSUS_RISK_DIR,
    owner_attribution_dir: Path = DEFAULT_OWNER_ATTRIBUTION_DIR,
    shadow_aging_dir: Path = DEFAULT_SHADOW_AGING_DIR,
    weekly_advisory_review_dir: Path = DEFAULT_WEEKLY_ADVISORY_REVIEW_DIR,
    advisory_outcome_dir: Path = DEFAULT_ADVISORY_OUTCOME_DIR,
    daily_advisory_dir: Path = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    owner_review_dir: Path = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    shadow_shortlist_dir: Path = DEFAULT_SHADOW_SHORTLIST_DIR,
    shadow_monitor_run_dir: Path = DEFAULT_SHADOW_MONITOR_RUN_DIR,
    consensus_drift_dir: Path = DEFAULT_CONSENSUS_DRIFT_DIR,
    paper_portfolio_dir: Path = DEFAULT_PAPER_PORTFOLIO_DIR,
    backfill_dir: Path = DEFAULT_BACKFILLED_OUTCOME_DIR,
    repair_dir: Path = DEFAULT_BACKFILL_REPAIR_DIR,
    historical_replay_dir: Path = DEFAULT_HISTORICAL_REPLAY_DIR,
    paper_sim_dir: Path = DEFAULT_HISTORICAL_PAPER_SIM_DIR,
    diagnosis_dir: Path = DEFAULT_REPLAY_DIAGNOSIS_DIR,
    outcome_due_dir: Path = DEFAULT_OUTCOME_DUE_DIR,
    config_path: Path = DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    shadow_shortlist_id: str | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _aware_utc(generated_at or datetime.now(UTC), "generated_at")
    _recover_prepared_rolling_refreshes(
        output_dir=output_dir,
        outcome_update_id=outcome_update_id,
    )
    if _committed_rolling_refresh_for_update(output_dir, outcome_update_id):
        raise DynamicV3OutcomeAccumulationError(
            "outcome update already has a COMMITTED rolling evidence refresh"
        )
    update_validation = validate_outcome_update_artifact(
        update_id=outcome_update_id,
        output_dir=outcome_update_dir,
    )
    if update_validation.get("status") != "PASS":
        raise DynamicV3OutcomeAccumulationError(
            f"outcome update validation failed: {update_validation.get('status')}"
        )
    update_root = _safe_child_dir(outcome_update_dir, outcome_update_id, "outcome_update_id")
    update_bundle = _immutable_source_bundle(update_root)
    update_manifest = _mapping(
        _source_bundle_content(update_bundle, "outcome_update_manifest.json")
    )
    update_generated = _datetime_from_any(update_manifest.get("generated_at"))
    as_of = _date_from_any(update_manifest.get("as_of"))
    if (
        update_manifest.get("outcome_update_id") != outcome_update_id
        or update_manifest.get("transaction_status") != "COMMITTED"
        or update_generated is None
        or as_of is None
        or generated < update_generated
        or as_of > generated.date()
    ):
        raise DynamicV3OutcomeAccumulationError(
            "outcome update identity/time/transaction boundary invalid"
        )
    baseline = _rolling_refresh_baseline(
        update_bundle=update_bundle,
        limited_vs_notrade_dir=limited_vs_notrade_dir,
        consensus_risk_dir=consensus_risk_dir,
    )
    output_roots = {
        "outcome_dashboard": outcome_dashboard_dir,
        "limited_vs_notrade": limited_vs_notrade_dir,
        "consensus_risk": consensus_risk_dir,
        "owner_attribution": owner_attribution_dir,
        "shadow_aging": shadow_aging_dir,
        "weekly_advisory_review": weekly_advisory_review_dir,
    }
    refresh_id = _stable_id("rolling-evidence-refresh-v2", outcome_update_id, generated.isoformat())
    refresh_dir = _unique_dir(output_dir / refresh_id)
    refresh_dir.mkdir(parents=True, exist_ok=False)
    transaction_path = refresh_dir / "rolling_refresh_transaction.json"
    prestate = _rolling_refresh_transaction_prestate(output_roots)
    transaction = _rolling_refresh_transaction(
        refresh_id=refresh_dir.name,
        outcome_update_id=outcome_update_id,
        generated=generated,
        status="PREPARED",
        prestate=prestate,
        created_artifacts={},
    )
    _write_json(transaction_path, transaction)
    created: dict[str, dict[str, str]] = {}
    try:
        dashboard = build_outcome_dashboard(
            output_dir=outcome_dashboard_dir,
            advisory_outcome_dir=advisory_outcome_dir,
            backfill_dir=backfill_dir,
            repair_dir=repair_dir,
            paper_sim_dir=paper_sim_dir,
            diagnosis_dir=diagnosis_dir,
            outcome_due_dir=outcome_due_dir,
            include_outcome_due_sources=False,
            generated_at=generated,
        )
        _record_rolling_refresh_artifact(
            created, "outcome_dashboard", outcome_dashboard_dir, dashboard["dashboard_id"]
        )
        limited = run_limited_vs_notrade_evaluation(
            output_dir=limited_vs_notrade_dir,
            advisory_outcome_dir=advisory_outcome_dir,
            backfill_dir=backfill_dir,
            repair_dir=repair_dir,
            generated_at=generated,
        )
        _record_rolling_refresh_artifact(
            created, "limited_vs_notrade", limited_vs_notrade_dir, limited["focus_id"]
        )
        consensus = run_consensus_risk_review(
            output_dir=consensus_risk_dir,
            daily_advisory_dir=daily_advisory_dir,
            historical_replay_dir=historical_replay_dir,
            backfill_dir=backfill_dir,
            repair_dir=repair_dir,
            generated_at=generated,
        )
        _record_rolling_refresh_artifact(
            created, "consensus_risk", consensus_risk_dir, consensus["risk_id"]
        )
        owner = run_owner_attribution(
            output_dir=owner_attribution_dir,
            owner_review_dir=owner_review_dir,
            outcome_dir=advisory_outcome_dir,
            generated_at=generated,
        )
        _record_rolling_refresh_artifact(
            created, "owner_attribution", owner_attribution_dir, owner["attribution_id"]
        )
        resolved_shadow_shortlist_id = shadow_shortlist_id or _resolve_shadow_shortlist_id(
            shadow_aging_dir
        )
        shadow: dict[str, Any] | None = None
        if resolved_shadow_shortlist_id:
            shadow = run_shadow_aging(
                shadow_shortlist_id=resolved_shadow_shortlist_id,
                config_path=config_path,
                output_dir=shadow_aging_dir,
                shadow_shortlist_dir=shadow_shortlist_dir,
                shadow_monitor_run_dir=shadow_monitor_run_dir,
                consensus_drift_dir=consensus_drift_dir,
                advisory_outcome_dir=advisory_outcome_dir,
                generated_at=generated,
            )
            _record_rolling_refresh_artifact(
                created, "shadow_aging", shadow_aging_dir, shadow["aging_id"]
            )
        weekly = run_weekly_advisory_review(
            week_ending=as_of,
            output_dir=weekly_advisory_review_dir,
            shadow_monitor_run_dir=shadow_monitor_run_dir,
            daily_advisory_dir=daily_advisory_dir,
            owner_review_dir=owner_review_dir,
            paper_portfolio_dir=paper_portfolio_dir,
            advisory_outcome_dir=advisory_outcome_dir,
            shadow_aging_dir=shadow_aging_dir,
            config_path=config_path,
            generated_at=generated,
        )
        _record_rolling_refresh_artifact(
            created,
            "weekly_advisory_review",
            weekly_advisory_review_dir,
            weekly["weekly_review_id"],
        )
        transaction = _rolling_refresh_transaction(
            refresh_id=refresh_dir.name,
            outcome_update_id=outcome_update_id,
            generated=generated,
            status="PREPARED",
            prestate=prestate,
            created_artifacts=created,
        )
        _write_json(transaction_path, transaction)
        validations = _rolling_refresh_downstream_validations(
            dashboard=dashboard,
            outcome_dashboard_dir=outcome_dashboard_dir,
            limited=limited,
            limited_vs_notrade_dir=limited_vs_notrade_dir,
            consensus=consensus,
            consensus_risk_dir=consensus_risk_dir,
            owner=owner,
            owner_attribution_dir=owner_attribution_dir,
            shadow=shadow,
            shadow_aging_dir=shadow_aging_dir,
            weekly=weekly,
            weekly_advisory_review_dir=weekly_advisory_review_dir,
        )
        failed = {
            key: value.get("status")
            for key, value in validations.items()
            if value.get("status") not in {"PASS", "SKIPPED_NO_SHADOW_SHORTLIST"}
        }
        if failed:
            raise DynamicV3OutcomeAccumulationError(
                f"rolling refresh downstream validation failed: {failed}"
            )
        post_artifacts = _rolling_refresh_post_artifacts(
            created=created,
            validations=validations,
        )
        snapshot = {
            "schema_version": ROLLING_EVIDENCE_REFRESH_SNAPSHOT_SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_rolling_evidence_refresh_source_snapshot",
            "refresh_id": refresh_dir.name,
            "outcome_update_id": outcome_update_id,
            "as_of": as_of.isoformat(),
            "generated_at": generated.isoformat(),
            "update_source_bundle": update_bundle,
            "update_validation": update_validation,
            "baseline": baseline,
            "post_artifacts": post_artifacts,
            "shadow_shortlist_id": resolved_shadow_shortlist_id,
            "production_effect": "none",
            "broker_action_taken": False,
        }
        refreshed = _rolling_refresh_refreshed_artifacts(snapshot)
        evidence_delta = _rolling_evidence_delta_from_snapshot(snapshot)
        snapshot_path = refresh_dir / "rolling_refresh_source_snapshot.json"
        _write_json(snapshot_path, snapshot)
        manifest = _rolling_refresh_manifest(
            refresh_dir=refresh_dir,
            outcome_update_id=outcome_update_id,
            as_of=as_of,
            generated=generated,
            evidence_delta=evidence_delta,
            source_snapshot_checksum=_file_sha256(snapshot_path),
        )
        _write_json(refresh_dir / "rolling_refresh_manifest.json", manifest)
        _write_json(refresh_dir / "refreshed_artifacts.json", refreshed)
        _write_json(refresh_dir / "evidence_delta_summary.json", evidence_delta)
        _write_text(
            refresh_dir / "rolling_evidence_refresh_report.md",
            render_rolling_evidence_refresh_report(manifest, refreshed, evidence_delta),
        )
        _write_text(
            refresh_dir / "reader_brief_section.md",
            render_rolling_refresh_reader_brief(manifest, evidence_delta),
        )
        transaction = _rolling_refresh_transaction(
            refresh_id=refresh_dir.name,
            outcome_update_id=outcome_update_id,
            generated=generated,
            status="COMMITTED",
            prestate=prestate,
            created_artifacts=created,
        )
        _write_json(transaction_path, transaction)
        final_validation = validate_rolling_evidence_refresh_artifact(
            refresh_id=refresh_dir.name,
            output_dir=output_dir,
        )
        if final_validation.get("status") != "PASS":
            raise DynamicV3OutcomeAccumulationError(
                "rolling evidence refresh self-validation failed"
            )
        _update_latest_pointer(
            "latest_rolling_evidence_refresh",
            refresh_dir.name,
            refresh_dir / "rolling_refresh_manifest.json",
        )
        return {
            "refresh_id": refresh_dir.name,
            "refresh_dir": refresh_dir,
            "manifest": manifest,
            "refreshed_artifacts": refreshed,
            "evidence_delta_summary": evidence_delta,
            "source_snapshot": snapshot,
            "transaction": transaction,
        }
    except Exception as exc:
        rollback = _rollback_rolling_refresh(prestate=prestate, created_artifacts=created)
        transaction = _rolling_refresh_transaction(
            refresh_id=refresh_dir.name,
            outcome_update_id=outcome_update_id,
            generated=generated,
            status="ROLLED_BACK",
            prestate=prestate,
            created_artifacts=created,
            error=str(exc),
            rollback_validation=rollback,
        )
        _write_json(transaction_path, transaction)
        raise


def rolling_evidence_refresh_report_payload(
    *,
    refresh_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_ROLLING_EVIDENCE_REFRESH_DIR,
) -> dict[str, Any]:
    refresh_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=refresh_id if not latest else None,
        pointer_name="latest_rolling_evidence_refresh",
    )
    return {
        **_read_json(refresh_dir / "rolling_refresh_manifest.json"),
        "refreshed_artifacts": _read_json(refresh_dir / "refreshed_artifacts.json"),
        "evidence_delta_summary": _read_json(refresh_dir / "evidence_delta_summary.json"),
        "source_snapshot": _read_optional_json(
            refresh_dir / "rolling_refresh_source_snapshot.json"
        ),
        "transaction": _read_optional_json(refresh_dir / "rolling_refresh_transaction.json"),
        "reader_brief_section": _read_text(refresh_dir / "reader_brief_section.md"),
        "refresh_dir": str(refresh_dir),
    }


def validate_rolling_evidence_refresh_artifact(
    *, refresh_id: str, output_dir: Path = DEFAULT_ROLLING_EVIDENCE_REFRESH_DIR
) -> dict[str, Any]:
    refresh_dir = output_dir / refresh_id
    manifest = _read_optional_json(refresh_dir / "rolling_refresh_manifest.json") or {}
    refreshed = _read_optional_json(refresh_dir / "refreshed_artifacts.json") or {}
    delta = _read_optional_json(refresh_dir / "evidence_delta_summary.json") or {}
    snapshot_path = refresh_dir / "rolling_refresh_source_snapshot.json"
    transaction_path = refresh_dir / "rolling_refresh_transaction.json"
    if not snapshot_path.is_file():
        legacy_checks = [
            _check(
                "legacy_required_files",
                all(
                    (refresh_dir / name).is_file()
                    for name in (
                        "rolling_refresh_manifest.json",
                        "refreshed_artifacts.json",
                        "evidence_delta_summary.json",
                        "rolling_evidence_refresh_report.md",
                        "reader_brief_section.md",
                    )
                ),
                refresh_id,
            ),
            _check("refresh_id_matches", manifest.get("refresh_id") == refresh_id, refresh_id),
            _check(
                "legacy_broker_action_forbidden",
                manifest.get("broker_action_allowed") is False
                and manifest.get("broker_action_taken") is False,
                "legacy refresh is evidence only",
            ),
        ]
        payload = _validation_payload(
            report_type="etf_dynamic_v3_rolling_evidence_refresh_validation",
            artifact_id_key="refresh_id",
            artifact_id=refresh_id,
            checks=legacy_checks,
        )
        if payload["status"] == "PASS":
            payload["status"] = "PASS_WITH_WARNINGS"
        payload["source_snapshot_status"] = "LEGACY_UNSNAPSHOTTED"
        return payload
    snapshot = _read_optional_json(snapshot_path) or {}
    transaction = _read_optional_json(transaction_path) or {}
    snapshot_errors = _rolling_refresh_snapshot_errors(snapshot)
    recompute_error = ""
    expected_refreshed: dict[str, Any] = {}
    expected_delta: dict[str, Any] = {}
    expected_manifest: dict[str, Any] = {}
    expected_report = ""
    expected_reader_brief = ""
    try:
        generated = _datetime_from_any(snapshot.get("generated_at"))
        as_of = _date_from_any(snapshot.get("as_of"))
        if generated is None or as_of is None:
            raise DynamicV3OutcomeAccumulationError("refresh snapshot time invalid")
        expected_refreshed = _rolling_refresh_refreshed_artifacts(snapshot)
        expected_delta = _rolling_evidence_delta_from_snapshot(snapshot)
        expected_manifest = _rolling_refresh_manifest(
            refresh_dir=refresh_dir,
            outcome_update_id=_text(snapshot.get("outcome_update_id")),
            as_of=as_of,
            generated=generated,
            evidence_delta=expected_delta,
            source_snapshot_checksum=_file_sha256(snapshot_path),
        )
        expected_report = render_rolling_evidence_refresh_report(
            expected_manifest, expected_refreshed, expected_delta
        )
        expected_reader_brief = render_rolling_refresh_reader_brief(
            expected_manifest, expected_delta
        )
    except Exception as exc:  # noqa: BLE001
        recompute_error = str(exc)
    checks = [
        _check("snapshot_content_valid", not snapshot_errors, ";".join(snapshot_errors)),
        _check("views_recomputed", not recompute_error, recompute_error),
        _check("refreshed_artifacts_recomputed", refreshed == expected_refreshed, "snapshot"),
        _check("evidence_delta_recomputed", delta == expected_delta, "snapshot"),
        _check("manifest_recomputed", manifest == expected_manifest, "snapshot"),
        _check(
            "report_recomputed",
            _read_text(refresh_dir / "rolling_evidence_refresh_report.md") == expected_report,
            "Markdown",
        ),
        _check(
            "reader_brief_recomputed",
            _read_text(refresh_dir / "reader_brief_section.md") == expected_reader_brief,
            "Reader Brief section",
        ),
        _check(
            "transaction_committed",
            transaction.get("schema_version")
            == ROLLING_EVIDENCE_REFRESH_TRANSACTION_SCHEMA_VERSION
            and transaction.get("status") == "COMMITTED"
            and transaction.get("refresh_id") == refresh_id
            and transaction.get("outcome_update_id") == snapshot.get("outcome_update_id")
            and transaction.get("rollback_required") is False,
            _text(transaction.get("status")),
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action forbidden",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_rolling_evidence_refresh_validation",
        artifact_id_key="refresh_id",
        artifact_id=refresh_id,
        checks=checks,
    )


def _rolling_refresh_baseline(
    *,
    update_bundle: Mapping[str, Any],
    limited_vs_notrade_dir: Path,
    consensus_risk_dir: Path,
) -> dict[str, Any]:
    update_delta = _mapping(
        _source_bundle_content(update_bundle, "outcome_status_delta.json")
    )
    return {
        "forward_selected_cohort": _mapping(update_delta.get("before")),
        "limited_vs_notrade": _rolling_refresh_latest_baseline_artifact(
            kind="limited_vs_notrade", output_dir=limited_vs_notrade_dir
        ),
        "consensus_risk": _rolling_refresh_latest_baseline_artifact(
            kind="consensus_risk", output_dir=consensus_risk_dir
        ),
    }


def _rolling_refresh_latest_baseline_artifact(
    *, kind: str, output_dir: Path
) -> dict[str, Any]:
    try:
        if kind == "limited_vs_notrade":
            payload = limited_vs_notrade_report_payload(latest=True, output_dir=output_dir)
            artifact_id = _text(payload.get("focus_id"))
            validation = validate_limited_vs_notrade_artifact(
                focus_id=artifact_id, output_dir=output_dir
            )
        elif kind == "consensus_risk":
            payload = consensus_risk_report_payload(latest=True, output_dir=output_dir)
            artifact_id = _text(payload.get("risk_id"))
            validation = validate_consensus_risk_artifact(
                risk_id=artifact_id, output_dir=output_dir
            )
        else:
            raise DynamicV3OutcomeAccumulationError(
                f"unsupported rolling refresh baseline kind: {kind}"
            )
    except DynamicV3OutcomeAccumulationError:
        return {"status": "MISSING", "artifact_id": "", "summary": None}
    if validation.get("status") != "PASS":
        raise DynamicV3OutcomeAccumulationError(
            f"rolling refresh baseline validation failed: {kind}={validation.get('status')}"
        )
    artifact_root = _safe_child_dir(output_dir, artifact_id, f"{kind}_id")
    bundle = _immutable_source_bundle(artifact_root)
    if kind == "limited_vs_notrade":
        metrics = _mapping(
            _source_bundle_content(bundle, "window_comparison_metrics.json")
        )
        rows = _records(metrics.get("by_window"))
        first = rows[0] if rows else {}
        summary: dict[str, Any] = {
            "available_count": _mapping(
                _source_bundle_content(bundle, "limited_vs_notrade_manifest.json")
            ).get("available_count"),
            "confidence": first.get("confidence"),
            "recommendation": metrics.get("overall_recommendation"),
        }
    else:
        summary = {
            "consensus_target_risk": _mapping(
                _source_bundle_content(bundle, "consensus_risk_manifest.json")
            ).get("consensus_target_risk")
        }
    return {
        "status": "PRESENT",
        "artifact_id": artifact_id,
        "root": str(artifact_root),
        "summary": summary,
        "source_bundle": bundle,
        "validation": validation,
    }


def _rolling_refresh_transaction_prestate(
    output_roots: Mapping[str, Path],
) -> dict[str, Any]:
    outputs = {
        key: {
            "root": str(root),
            "child_names": [child.name for child in _artifact_children(root)],
        }
        for key, root in output_roots.items()
    }
    pointer_names = (
        "latest_outcome_dashboard",
        "latest_limited_vs_notrade",
        "latest_consensus_risk",
        "latest_owner_attribution",
        "latest_shadow_aging",
        "latest_weekly_advisory_review",
    )
    pointers: dict[str, Any] = {}
    for name in pointer_names:
        pointer_root = (
            dynamic_v3_paper_tracking.DEFAULT_DYNAMIC_V3_LATEST_POINTER_DIR
            if name
            in {
                "latest_owner_attribution",
                "latest_shadow_aging",
                "latest_weekly_advisory_review",
            }
            else DEFAULT_LATEST_POINTER_DIR
        )
        path = pointer_root / f"{name}.json"
        pointers[name] = {
            "path": str(path),
            "existed": path.is_file(),
            "content": _read_optional_json(path),
        }
    return {"outputs": outputs, "latest_pointers": pointers}


def _rolling_refresh_transaction(
    *,
    refresh_id: str,
    outcome_update_id: str,
    generated: datetime,
    status: str,
    prestate: Mapping[str, Any],
    created_artifacts: Mapping[str, Any],
    error: str = "",
    rollback_validation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if status not in {"PREPARED", "COMMITTED", "ROLLED_BACK"}:
        raise DynamicV3OutcomeAccumulationError(
            "rolling evidence refresh transaction status invalid"
        )
    return {
        "schema_version": ROLLING_EVIDENCE_REFRESH_TRANSACTION_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_rolling_evidence_refresh_transaction",
        "refresh_id": refresh_id,
        "outcome_update_id": outcome_update_id,
        "transaction_at": generated.isoformat(),
        "status": status,
        "prestate": dict(prestate),
        "created_artifacts": dict(created_artifacts),
        "rollback_required": status == "PREPARED",
        "rollback_validation": dict(rollback_validation or {}),
        "error": error,
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _record_rolling_refresh_artifact(
    created: dict[str, dict[str, str]], kind: str, root: Path, artifact_id: str
) -> None:
    created[kind] = {"root": str(root), "artifact_id": artifact_id}


def _rolling_refresh_downstream_validations(
    *,
    dashboard: Mapping[str, Any],
    outcome_dashboard_dir: Path,
    limited: Mapping[str, Any],
    limited_vs_notrade_dir: Path,
    consensus: Mapping[str, Any],
    consensus_risk_dir: Path,
    owner: Mapping[str, Any],
    owner_attribution_dir: Path,
    shadow: Mapping[str, Any] | None,
    shadow_aging_dir: Path,
    weekly: Mapping[str, Any],
    weekly_advisory_review_dir: Path,
) -> dict[str, dict[str, Any]]:
    validations = {
        "outcome_dashboard": validate_outcome_dashboard_artifact(
            dashboard_id=_text(dashboard.get("dashboard_id")),
            output_dir=outcome_dashboard_dir,
        ),
        "limited_vs_notrade": validate_limited_vs_notrade_artifact(
            focus_id=_text(limited.get("focus_id")),
            output_dir=limited_vs_notrade_dir,
        ),
        "consensus_risk": validate_consensus_risk_artifact(
            risk_id=_text(consensus.get("risk_id")),
            output_dir=consensus_risk_dir,
        ),
        "owner_attribution": validate_owner_attribution_artifact(
            attribution_id=_text(owner.get("attribution_id")),
            output_dir=owner_attribution_dir,
        ),
        "weekly_advisory_review": validate_weekly_advisory_review_artifact(
            weekly_review_id=_text(weekly.get("weekly_review_id")),
            output_dir=weekly_advisory_review_dir,
        ),
    }
    validations["shadow_aging"] = (
        {
            "status": "SKIPPED_NO_SHADOW_SHORTLIST",
            "failed_check_count": 0,
            "production_effect": "none",
            "broker_action_taken": False,
        }
        if shadow is None
        else validate_shadow_aging_artifact(
            aging_id=_text(shadow.get("aging_id")), output_dir=shadow_aging_dir
        )
    )
    return validations


def _rolling_refresh_post_artifacts(
    *,
    created: Mapping[str, Mapping[str, str]],
    validations: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    artifacts: dict[str, Any] = {}
    for kind in (
        "outcome_dashboard",
        "limited_vs_notrade",
        "consensus_risk",
        "owner_attribution",
        "shadow_aging",
        "weekly_advisory_review",
    ):
        created_row = _mapping(created.get(kind))
        validation = _mapping(validations.get(kind))
        if kind == "shadow_aging" and not created_row:
            artifacts[kind] = {
                "status": "SKIPPED_NO_SHADOW_SHORTLIST",
                "artifact_id": "",
                "validation": validation,
            }
            continue
        root = Path(_text(created_row.get("root")))
        artifact_id = _text(created_row.get("artifact_id"))
        artifact_root = _safe_child_dir(root, artifact_id, f"{kind}_id")
        artifacts[kind] = {
            "status": "REFRESHED",
            "artifact_id": artifact_id,
            "root": str(artifact_root),
            "source_bundle": _immutable_source_bundle(artifact_root),
            "validation": validation,
        }
    return artifacts


def _rolling_refresh_refreshed_artifacts(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    post = _mapping(snapshot.get("post_artifacts"))
    required = {
        "outcome_dashboard_id": "outcome_dashboard",
        "limited_vs_notrade_id": "limited_vs_notrade",
        "consensus_risk_id": "consensus_risk",
        "owner_attribution_id": "owner_attribution",
        "weekly_advisory_review_id": "weekly_advisory_review",
    }
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_refreshed_artifacts",
        **{
            output_key: _text(_mapping(post.get(kind)).get("artifact_id"))
            for output_key, kind in required.items()
        },
        "shadow_aging_id": _text(
            _mapping(post.get("shadow_aging")).get("artifact_id")
        ),
        "shadow_aging_status": _text(
            _mapping(post.get("shadow_aging")).get("status")
        ),
        "reader_brief_section_generated": True,
        "reader_brief_updated": False,
        "all_downstream_validations_passed": all(
            _mapping(row).get("validation", {}).get("status")
            in {"PASS", "SKIPPED_NO_SHADOW_SHORTLIST"}
            for row in post.values()
        ),
        "production_effect": "none",
        "broker_action_taken": False,
    }
    if not all(_text(payload.get(key)) for key in required):
        raise DynamicV3OutcomeAccumulationError(
            "rolling refresh snapshot missing required downstream artifact id"
        )
    return payload


def _rolling_evidence_delta_from_snapshot(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    update_bundle = _mapping(snapshot.get("update_source_bundle"))
    update_delta = _mapping(
        _source_bundle_content(update_bundle, "outcome_status_delta.json")
    )
    baseline = _mapping(snapshot.get("baseline"))
    post = _mapping(snapshot.get("post_artifacts"))
    dashboard_bundle = _mapping(
        _mapping(post.get("outcome_dashboard")).get("source_bundle")
    )
    limited_bundle = _mapping(
        _mapping(post.get("limited_vs_notrade")).get("source_bundle")
    )
    consensus_bundle = _mapping(
        _mapping(post.get("consensus_risk")).get("source_bundle")
    )
    dashboard_matrix = _mapping(
        _source_bundle_content(dashboard_bundle, "outcome_availability_matrix.json")
    )
    dashboard_summary = _mapping(dashboard_matrix.get("summary"))
    historical = _mapping(dashboard_summary.get("historical_replay"))
    limited_manifest = _mapping(
        _source_bundle_content(limited_bundle, "limited_vs_notrade_manifest.json")
    )
    limited_metrics = _mapping(
        _source_bundle_content(limited_bundle, "window_comparison_metrics.json")
    )
    limited_rows = _records(limited_metrics.get("by_window"))
    limited_first = limited_rows[0] if limited_rows else {}
    consensus_manifest = _mapping(
        _source_bundle_content(consensus_bundle, "consensus_risk_manifest.json")
    )
    before_forward = _mapping(update_delta.get("before"))
    after_forward = _mapping(update_delta.get("after"))
    limited_baseline = _mapping(baseline.get("limited_vs_notrade"))
    consensus_baseline = _mapping(baseline.get("consensus_risk"))
    before_limited_summary = (
        _mapping(limited_baseline.get("summary"))
        if limited_baseline.get("status") == "PRESENT"
        else {}
    )
    before_consensus_summary = (
        _mapping(consensus_baseline.get("summary"))
        if consensus_baseline.get("status") == "PRESENT"
        else {}
    )
    before = {
        "forward_available": before_forward.get("forward_available"),
        "forward_pending": before_forward.get("forward_pending"),
        "limited_vs_notrade_baseline_status": limited_baseline.get("status"),
        "limited_vs_notrade_available_count": before_limited_summary.get("available_count"),
        "limited_vs_notrade_confidence": before_limited_summary.get("confidence"),
        "consensus_risk_baseline_status": consensus_baseline.get("status"),
        "consensus_target_risk": before_consensus_summary.get("consensus_target_risk"),
    }
    limited_confidence = limited_first.get("confidence")
    limited_recommendation = limited_metrics.get("overall_recommendation")
    after = {
        "forward_available": after_forward.get("forward_available"),
        "forward_pending": after_forward.get("forward_pending"),
        "historical_replay_available": historical.get("available"),
        "historical_replay_pending": historical.get("pending"),
        "limited_vs_notrade_available_count": limited_manifest.get("available_count"),
        "limited_vs_notrade_win_rate": limited_first.get("win_rate"),
        "limited_vs_notrade_avg_relative_return": limited_first.get(
            "avg_relative_return"
        ),
        "limited_vs_notrade_confidence": limited_confidence,
        "consensus_target_risk": consensus_manifest.get("consensus_target_risk"),
        "recommended_action": (
            "continue_tracking"
            if limited_confidence in {None, "LOW", "INSUFFICIENT_DATA"}
            else limited_recommendation
        ),
    }
    material_reasons = []
    for field in ("forward_available", "forward_pending"):
        if before.get(field) != after.get(field):
            material_reasons.append(f"{field}_changed")
    if (
        limited_baseline.get("status") == "PRESENT"
        and before.get("limited_vs_notrade_confidence")
        != after.get("limited_vs_notrade_confidence")
    ):
        material_reasons.append("limited_vs_notrade_confidence_changed")
    if (
        consensus_baseline.get("status") == "PRESENT"
        and before.get("consensus_target_risk") != after.get("consensus_target_risk")
    ):
        material_reasons.append("consensus_target_risk_changed")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_evidence_delta_summary",
        "scope": "outcome_update_selected_cohort_plus_transaction_baselines",
        "before": before,
        "after": after,
        "material_change": bool(material_reasons),
        "material_change_reasons": material_reasons,
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _rolling_refresh_manifest(
    *,
    refresh_dir: Path,
    outcome_update_id: str,
    as_of: date,
    generated: datetime,
    evidence_delta: Mapping[str, Any],
    source_snapshot_checksum: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_rolling_refresh_manifest",
        "refresh_id": refresh_dir.name,
        "outcome_update_id": outcome_update_id,
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "transaction_status": "COMMITTED",
        "material_change": evidence_delta.get("material_change"),
        "source_snapshot_path": str(refresh_dir / "rolling_refresh_source_snapshot.json"),
        "source_snapshot_checksum": source_snapshot_checksum,
        "transaction_path": str(refresh_dir / "rolling_refresh_transaction.json"),
        "rolling_refresh_manifest_path": str(refresh_dir / "rolling_refresh_manifest.json"),
        "refreshed_artifacts_path": str(refresh_dir / "refreshed_artifacts.json"),
        "evidence_delta_summary_path": str(refresh_dir / "evidence_delta_summary.json"),
        "rolling_evidence_refresh_report_path": str(
            refresh_dir / "rolling_evidence_refresh_report.md"
        ),
        "reader_brief_section_path": str(refresh_dir / "reader_brief_section.md"),
        "reader_brief_section_generated": True,
        "reader_brief_updated": False,
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _rolling_refresh_snapshot_errors(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if snapshot.get("schema_version") != ROLLING_EVIDENCE_REFRESH_SNAPSHOT_SCHEMA_VERSION:
        errors.append("snapshot_schema_invalid")
    generated = _datetime_from_any(snapshot.get("generated_at"))
    as_of = _date_from_any(snapshot.get("as_of"))
    update_bundle = _mapping(snapshot.get("update_source_bundle"))
    update_manifest = _mapping(
        _source_bundle_content(update_bundle, "outcome_update_manifest.json")
    )
    update_generated = _datetime_from_any(update_manifest.get("generated_at"))
    if (
        generated is None
        or as_of is None
        or update_generated is None
        or generated < update_generated
        or as_of > generated.date()
        or update_manifest.get("outcome_update_id") != snapshot.get("outcome_update_id")
        or update_manifest.get("transaction_status") != "COMMITTED"
    ):
        errors.append("update_identity_or_time_invalid")
    if not _source_bundle_matches(update_bundle):
        errors.append("update_source_bundle_changed")
    else:
        update_root = Path(_text(update_bundle.get("root")))
        update_validation = validate_outcome_update_artifact(
            update_id=_text(snapshot.get("outcome_update_id")),
            output_dir=update_root.parent,
        )
        if (
            update_validation.get("status") != "PASS"
            or _mapping(snapshot.get("update_validation")) != update_validation
        ):
            errors.append("live_update_validation_failed")
    for kind in ("limited_vs_notrade", "consensus_risk"):
        baseline = _mapping(_mapping(snapshot.get("baseline")).get(kind))
        if baseline.get("status") == "PRESENT":
            bundle = _mapping(baseline.get("source_bundle"))
            if not _source_bundle_matches(bundle):
                errors.append(f"{kind}_baseline_changed")
                continue
            live_validation = _validate_rolling_refresh_post_artifact(
                kind=kind,
                artifact_id=_text(baseline.get("artifact_id")),
                output_dir=Path(_text(baseline.get("root"))).parent,
            )
            expected_summary = _rolling_refresh_baseline_summary(kind=kind, bundle=bundle)
            if (
                live_validation.get("status") != "PASS"
                or _mapping(baseline.get("validation")) != live_validation
                or _mapping(baseline.get("summary")) != expected_summary
            ):
                errors.append(f"{kind}_baseline_validation_failed")
        elif baseline.get("status") != "MISSING":
            errors.append(f"{kind}_baseline_status_invalid")
    post = _mapping(snapshot.get("post_artifacts"))
    for kind in (
        "outcome_dashboard",
        "limited_vs_notrade",
        "consensus_risk",
        "owner_attribution",
        "shadow_aging",
        "weekly_advisory_review",
    ):
        row = _mapping(post.get(kind))
        if kind == "shadow_aging" and row.get("status") == "SKIPPED_NO_SHADOW_SHORTLIST":
            if _mapping(row.get("validation")).get("status") != (
                "SKIPPED_NO_SHADOW_SHORTLIST"
            ):
                errors.append("shadow_aging_skip_validation_invalid")
            continue
        bundle = _mapping(row.get("source_bundle"))
        if row.get("status") != "REFRESHED" or not _source_bundle_matches(bundle):
            errors.append(f"{kind}_post_bundle_changed")
            continue
        live_validation = _validate_rolling_refresh_post_artifact(
            kind=kind,
            artifact_id=_text(row.get("artifact_id")),
            output_dir=Path(_text(row.get("root"))).parent,
        )
        if (
            live_validation.get("status") != "PASS"
            or _mapping(row.get("validation")) != live_validation
        ):
            errors.append(f"{kind}_post_validation_failed")
    return errors


def _rolling_refresh_baseline_summary(
    *, kind: str, bundle: Mapping[str, Any]
) -> dict[str, Any]:
    if kind == "limited_vs_notrade":
        metrics = _mapping(
            _source_bundle_content(bundle, "window_comparison_metrics.json")
        )
        rows = _records(metrics.get("by_window"))
        first = rows[0] if rows else {}
        return {
            "available_count": _mapping(
                _source_bundle_content(bundle, "limited_vs_notrade_manifest.json")
            ).get("available_count"),
            "confidence": first.get("confidence"),
            "recommendation": metrics.get("overall_recommendation"),
        }
    if kind == "consensus_risk":
        return {
            "consensus_target_risk": _mapping(
                _source_bundle_content(bundle, "consensus_risk_manifest.json")
            ).get("consensus_target_risk")
        }
    raise DynamicV3OutcomeAccumulationError(
        f"unsupported rolling refresh baseline kind: {kind}"
    )


def _validate_rolling_refresh_post_artifact(
    *, kind: str, artifact_id: str, output_dir: Path
) -> dict[str, Any]:
    if kind == "outcome_dashboard":
        return validate_outcome_dashboard_artifact(
            dashboard_id=artifact_id, output_dir=output_dir
        )
    if kind == "limited_vs_notrade":
        return validate_limited_vs_notrade_artifact(
            focus_id=artifact_id, output_dir=output_dir
        )
    if kind == "consensus_risk":
        return validate_consensus_risk_artifact(risk_id=artifact_id, output_dir=output_dir)
    if kind == "owner_attribution":
        return validate_owner_attribution_artifact(
            attribution_id=artifact_id, output_dir=output_dir
        )
    if kind == "shadow_aging":
        return validate_shadow_aging_artifact(aging_id=artifact_id, output_dir=output_dir)
    if kind == "weekly_advisory_review":
        return validate_weekly_advisory_review_artifact(
            weekly_review_id=artifact_id, output_dir=output_dir
        )
    raise DynamicV3OutcomeAccumulationError(
        f"unsupported rolling refresh downstream kind: {kind}"
    )


def _rollback_rolling_refresh(
    *, prestate: Mapping[str, Any], created_artifacts: Mapping[str, Any]
) -> dict[str, Any]:
    output_status: dict[str, Any] = {}
    rollback_errors: list[str] = []
    for kind, raw_state in _mapping(prestate.get("outputs")).items():
        state = _mapping(raw_state)
        root = Path(_text(state.get("root")))
        before = set(_texts(state.get("child_names")))
        removed = []
        try:
            if root.exists():
                for child in _artifact_children(root):
                    if child.name not in before:
                        shutil.rmtree(child)
                        removed.append(child.name)
            output_status[kind] = {"removed_artifact_ids": removed, "restored": True}
        except Exception as exc:  # noqa: BLE001
            rollback_errors.append(f"output:{kind}:{exc}")
            output_status[kind] = {"removed_artifact_ids": removed, "restored": False}
    pointer_status: dict[str, str] = {}
    for name, raw_state in _mapping(prestate.get("latest_pointers")).items():
        state = _mapping(raw_state)
        path = Path(_text(state.get("path")))
        try:
            if state.get("existed") is True:
                content = _mapping(state.get("content"))
                if not content:
                    raise DynamicV3OutcomeAccumulationError(
                        f"rolling refresh pointer backup invalid: {name}"
                    )
                _write_json(path, content)
                pointer_status[name] = "RESTORED"
            else:
                if path.exists():
                    path.unlink()
                pointer_status[name] = "REMOVED"
        except Exception as exc:  # noqa: BLE001
            rollback_errors.append(f"pointer:{name}:{exc}")
            pointer_status[name] = "FAILED"
    return {
        "outputs": output_status,
        "latest_pointers": pointer_status,
        "created_artifact_count": len(created_artifacts),
        "errors": rollback_errors,
        "status": "PASS" if not rollback_errors else "FAIL",
    }


def _recover_prepared_rolling_refreshes(
    *, output_dir: Path, outcome_update_id: str
) -> None:
    if not output_dir.exists():
        return
    for child in _artifact_children(output_dir):
        transaction_path = child / "rolling_refresh_transaction.json"
        transaction = _read_optional_json(transaction_path) or {}
        if (
            transaction.get("outcome_update_id") != outcome_update_id
            or transaction.get("status") != "PREPARED"
        ):
            continue
        generated = _datetime_from_any(transaction.get("transaction_at"))
        if generated is None:
            raise DynamicV3OutcomeAccumulationError(
                f"prepared rolling refresh time invalid: {child.name}"
            )
        rollback = _rollback_rolling_refresh(
            prestate=_mapping(transaction.get("prestate")),
            created_artifacts=_mapping(transaction.get("created_artifacts")),
        )
        recovered = _rolling_refresh_transaction(
            refresh_id=child.name,
            outcome_update_id=outcome_update_id,
            generated=generated,
            status="ROLLED_BACK",
            prestate=_mapping(transaction.get("prestate")),
            created_artifacts=_mapping(transaction.get("created_artifacts")),
            error="recovered_prepared_transaction_before_retry",
            rollback_validation=rollback,
        )
        _write_json(transaction_path, recovered)


def _committed_rolling_refresh_for_update(
    output_dir: Path, outcome_update_id: str
) -> bool:
    if not output_dir.exists():
        return False
    committed = []
    for child in _artifact_children(output_dir):
        transaction = _read_optional_json(child / "rolling_refresh_transaction.json") or {}
        if (
            transaction.get("outcome_update_id") == outcome_update_id
            and transaction.get("status") == "COMMITTED"
        ):
            committed.append(child.name)
    if len(committed) > 1:
        raise DynamicV3OutcomeAccumulationError(
            "multiple COMMITTED rolling refreshes found for one outcome update"
        )
    return bool(committed)


def run_evidence_trend(
    *,
    output_dir: Path = DEFAULT_EVIDENCE_TREND_DIR,
    rolling_refresh_dir: Path = DEFAULT_ROLLING_EVIDENCE_REFRESH_DIR,
    policy_path: Path = DEFAULT_EVIDENCE_TREND_POLICY_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _aware_utc(generated_at or datetime.now(UTC), "generated_at")
    policy = _load_evidence_trend_policy(policy_path)
    snapshot = _build_evidence_trend_snapshot(
        rolling_refresh_dir=rolling_refresh_dir,
        policy_path=policy_path,
        policy=policy,
        generated=generated,
    )
    timeseries = _evidence_trend_timeseries(snapshot)
    summary = _confidence_trend_summary(timeseries, policy)
    trend_id = _stable_id(
        "evidence-trend-v2",
        len(timeseries),
        generated.isoformat(),
        sha256(
            json.dumps(snapshot, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest(),
    )
    trend_dir = _unique_dir(output_dir / trend_id)
    trend_dir.mkdir(parents=True, exist_ok=False)
    snapshot["trend_id"] = trend_dir.name
    snapshot_path = trend_dir / "evidence_trend_source_snapshot.json"
    _write_json(snapshot_path, snapshot)
    manifest = _evidence_trend_manifest(
        trend_dir=trend_dir,
        generated=generated,
        timeseries=timeseries,
        summary=summary,
        policy=policy,
        source_snapshot_checksum=_file_sha256(snapshot_path),
    )
    _write_json(trend_dir / "evidence_trend_manifest.json", manifest)
    _write_jsonl(trend_dir / "evidence_trend_timeseries.jsonl", timeseries)
    _write_json(trend_dir / "confidence_trend_summary.json", summary)
    _write_text(
        trend_dir / "evidence_trend_report.md",
        render_evidence_trend_report(manifest, summary, timeseries),
    )
    _update_latest_pointer(
        "latest_evidence_trend",
        trend_dir.name,
        trend_dir / "evidence_trend_manifest.json",
    )
    return {
        "trend_id": trend_dir.name,
        "trend_dir": trend_dir,
        "manifest": manifest,
        "evidence_trend_timeseries": timeseries,
        "confidence_trend_summary": summary,
        "source_snapshot": snapshot,
    }


def evidence_trend_report_payload(
    *,
    trend_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_EVIDENCE_TREND_DIR,
) -> dict[str, Any]:
    trend_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=trend_id if not latest else None,
        pointer_name="latest_evidence_trend",
    )
    return {
        **_read_json(trend_dir / "evidence_trend_manifest.json"),
        "evidence_trend_timeseries": _read_jsonl(trend_dir / "evidence_trend_timeseries.jsonl"),
        "confidence_trend_summary": _read_json(trend_dir / "confidence_trend_summary.json"),
        "source_snapshot": _read_optional_json(
            trend_dir / "evidence_trend_source_snapshot.json"
        ),
        "trend_dir": str(trend_dir),
    }


def validate_evidence_trend_artifact(
    *, trend_id: str, output_dir: Path = DEFAULT_EVIDENCE_TREND_DIR
) -> dict[str, Any]:
    trend_dir = output_dir / trend_id
    manifest = _read_optional_json(trend_dir / "evidence_trend_manifest.json") or {}
    rows = _read_jsonl(trend_dir / "evidence_trend_timeseries.jsonl")
    summary = _read_optional_json(trend_dir / "confidence_trend_summary.json") or {}
    snapshot_path = trend_dir / "evidence_trend_source_snapshot.json"
    if not snapshot_path.is_file():
        legacy_checks = [
            _check(
                "legacy_required_files",
                all(
                    (trend_dir / name).is_file()
                    for name in (
                        "evidence_trend_manifest.json",
                        "evidence_trend_timeseries.jsonl",
                        "confidence_trend_summary.json",
                        "evidence_trend_report.md",
                    )
                ),
                trend_id,
            ),
            _check("trend_id_matches", manifest.get("trend_id") == trend_id, trend_id),
            _check(
                "legacy_broker_action_forbidden",
                manifest.get("broker_action_allowed") is False
                and manifest.get("broker_action_taken") is False,
                "legacy trend is observation only",
            ),
        ]
        payload = _validation_payload(
            report_type="etf_dynamic_v3_evidence_trend_validation",
            artifact_id_key="trend_id",
            artifact_id=trend_id,
            checks=legacy_checks,
        )
        if payload["status"] == "PASS":
            payload["status"] = "PASS_WITH_WARNINGS"
        payload["source_snapshot_status"] = "LEGACY_UNSNAPSHOTTED"
        return payload
    snapshot = _read_optional_json(snapshot_path) or {}
    snapshot_errors = _evidence_trend_snapshot_errors(snapshot)
    recompute_error = ""
    expected_rows: list[dict[str, Any]] = []
    expected_summary: dict[str, Any] = {}
    expected_manifest: dict[str, Any] = {}
    expected_report = ""
    try:
        generated = _datetime_from_any(snapshot.get("generated_at"))
        if generated is None:
            raise DynamicV3OutcomeAccumulationError("trend snapshot generated_at invalid")
        policy = _mapping(snapshot.get("policy"))
        expected_rows = _evidence_trend_timeseries(snapshot)
        expected_summary = _confidence_trend_summary(expected_rows, policy)
        expected_manifest = _evidence_trend_manifest(
            trend_dir=trend_dir,
            generated=generated,
            timeseries=expected_rows,
            summary=expected_summary,
            policy=policy,
            source_snapshot_checksum=_file_sha256(snapshot_path),
        )
        expected_report = render_evidence_trend_report(
            expected_manifest, expected_summary, expected_rows
        )
    except Exception as exc:  # noqa: BLE001
        recompute_error = str(exc)
    checks = [
        _check("snapshot_content_valid", not snapshot_errors, ";".join(snapshot_errors)),
        _check("views_recomputed", not recompute_error, recompute_error),
        _check("timeseries_recomputed", rows == expected_rows, "snapshot"),
        _check("summary_recomputed", summary == expected_summary, "snapshot"),
        _check("manifest_recomputed", manifest == expected_manifest, "snapshot"),
        _check(
            "report_recomputed",
            _read_text(trend_dir / "evidence_trend_report.md") == expected_report,
            "Markdown",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action forbidden",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_evidence_trend_validation",
        artifact_id_key="trend_id",
        artifact_id=trend_id,
        checks=checks,
    )


def run_forward_outcome_decision(
    *,
    week_ending: date,
    output_dir: Path = DEFAULT_FORWARD_OUTCOME_DECISION_DIR,
    outcome_update_dir: Path = DEFAULT_OUTCOME_UPDATE_DIR,
    rolling_refresh_dir: Path = DEFAULT_ROLLING_EVIDENCE_REFRESH_DIR,
    evidence_trend_dir: Path = DEFAULT_EVIDENCE_TREND_DIR,
    outcome_update_id: str | None = None,
    refresh_id: str | None = None,
    trend_id: str | None = None,
    policy_path: Path = DEFAULT_FORWARD_OUTCOME_DECISION_POLICY_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    policy = _load_forward_outcome_decision_policy(policy_path)
    snapshot = _build_forward_outcome_decision_snapshot(
        week_ending=week_ending,
        generated=generated,
        outcome_update_dir=outcome_update_dir,
        rolling_refresh_dir=rolling_refresh_dir,
        evidence_trend_dir=evidence_trend_dir,
        outcome_update_id=outcome_update_id,
        refresh_id=refresh_id,
        trend_id=trend_id,
        policy_path=policy_path,
        policy=policy,
    )
    matrix = _forward_go_no_go_matrix(
        week_ending=week_ending,
        snapshot=snapshot,
        policy=policy,
    )
    next_actions = _forward_next_actions(matrix, policy)
    decision_id = _stable_id(
        "forward-outcome-decision",
        week_ending.isoformat(),
        generated.isoformat(),
    )
    decision_dir = _unique_dir(output_dir / decision_id)
    decision_dir.mkdir(parents=True, exist_ok=False)
    snapshot_path = decision_dir / "forward_decision_source_snapshot.json"
    _write_json(snapshot_path, snapshot)
    manifest = _forward_outcome_decision_manifest(
        decision_dir=decision_dir,
        week_ending=week_ending,
        generated=generated,
        matrix=matrix,
        policy=policy,
        source_snapshot_checksum=_file_sha256(snapshot_path),
    )
    _write_json(decision_dir / "forward_decision_manifest.json", manifest)
    _write_json(decision_dir / "forward_go_no_go_matrix.json", matrix)
    _write_json(decision_dir / "forward_next_actions.json", next_actions)
    _write_text(
        decision_dir / "forward_outcome_decision_report.md",
        render_forward_outcome_decision_report(manifest, matrix, next_actions),
    )
    _write_text(
        decision_dir / "reader_brief_section.md",
        render_forward_decision_reader_brief(matrix, next_actions),
    )
    _update_latest_pointer(
        "latest_forward_outcome_decision",
        decision_dir.name,
        decision_dir / "forward_decision_manifest.json",
    )
    return {
        "decision_id": decision_dir.name,
        "decision_dir": decision_dir,
        "manifest": manifest,
        "source_snapshot": snapshot,
        "forward_go_no_go_matrix": matrix,
        "forward_next_actions": next_actions,
    }


def forward_outcome_decision_report_payload(
    *,
    decision_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FORWARD_OUTCOME_DECISION_DIR,
) -> dict[str, Any]:
    decision_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=decision_id if not latest else None,
        pointer_name="latest_forward_outcome_decision",
    )
    payload = {
        **_read_json(decision_dir / "forward_decision_manifest.json"),
        "forward_go_no_go_matrix": _read_json(decision_dir / "forward_go_no_go_matrix.json"),
        "forward_next_actions": _read_json(decision_dir / "forward_next_actions.json"),
        "reader_brief_section": _read_text(decision_dir / "reader_brief_section.md"),
        "decision_dir": str(decision_dir),
    }
    snapshot_path = decision_dir / "forward_decision_source_snapshot.json"
    if snapshot_path.exists():
        payload["source_snapshot"] = _read_json(snapshot_path)
    return payload


def validate_forward_outcome_decision_artifact(
    *, decision_id: str, output_dir: Path = DEFAULT_FORWARD_OUTCOME_DECISION_DIR
) -> dict[str, Any]:
    decision_dir = output_dir / decision_id
    manifest = _read_optional_json(decision_dir / "forward_decision_manifest.json") or {}
    matrix = _read_optional_json(decision_dir / "forward_go_no_go_matrix.json") or {}
    snapshot_path = decision_dir / "forward_decision_source_snapshot.json"
    if not snapshot_path.exists():
        legacy_checks = [
            _check("manifest_exists", bool(manifest), decision_id),
            _check("decision_id_matches", manifest.get("decision_id") == decision_id, decision_id),
            _check(
                "legacy_safety",
                manifest.get("production_effect") == "none"
                and manifest.get("broker_action_allowed") is False
                and matrix.get("production_effect") == "none"
                and matrix.get("broker_action_allowed") is False,
                "legacy forward outcome decision safety",
            ),
        ]
        payload = _validation_payload(
            report_type="etf_dynamic_v3_forward_outcome_decision_validation",
            artifact_id_key="decision_id",
            artifact_id=decision_id,
            checks=legacy_checks,
        )
        if payload["status"] == "PASS":
            payload["status"] = "PASS_WITH_WARNINGS"
            payload["warning"] = "LEGACY_UNSNAPSHOTTED"
        return payload
    snapshot = _read_json(snapshot_path)
    next_actions = _read_optional_json(decision_dir / "forward_next_actions.json") or {}
    snapshot_errors = _forward_outcome_decision_snapshot_errors(snapshot)
    report_path = decision_dir / "forward_outcome_decision_report.md"
    reader_path = decision_dir / "reader_brief_section.md"
    actual_report = _read_text(report_path) if report_path.exists() else ""
    actual_reader = _read_text(reader_path) if reader_path.exists() else ""
    recompute_error = ""
    try:
        policy = _mapping(snapshot.get("policy"))
        generated = _datetime_from_any(manifest.get("generated_at"))
        week_ending = _date_from_any(manifest.get("week_ending"))
        if generated is None or week_ending is None:
            raise DynamicV3OutcomeAccumulationError("forward decision manifest time invalid")
        expected_matrix = _forward_go_no_go_matrix(
            week_ending=week_ending,
            snapshot=snapshot,
            policy=policy,
        )
        expected_actions = _forward_next_actions(expected_matrix, policy)
        expected_manifest = _forward_outcome_decision_manifest(
            decision_dir=decision_dir,
            week_ending=week_ending,
            generated=generated,
            matrix=expected_matrix,
            policy=policy,
            source_snapshot_checksum=_file_sha256(snapshot_path),
        )
        expected_report = render_forward_outcome_decision_report(
            expected_manifest, expected_matrix, expected_actions
        )
        expected_reader = render_forward_decision_reader_brief(
            expected_matrix, expected_actions
        )
    except Exception as exc:  # noqa: BLE001
        recompute_error = str(exc)
        expected_matrix = {}
        expected_actions = {}
        expected_manifest = {}
        expected_report = ""
        expected_reader = ""
    checks = [
        _check(
            "manifest_exists",
            (decision_dir / "forward_decision_manifest.json").exists(),
            decision_id,
        ),
        _check(
            "source_snapshot_exists",
            snapshot_path.exists(),
            decision_id,
        ),
        _check(
            "go_no_go_exists",
            (decision_dir / "forward_go_no_go_matrix.json").exists(),
            decision_id,
        ),
        _check(
            "next_actions_exists",
            (decision_dir / "forward_next_actions.json").exists(),
            decision_id,
        ),
        _check(
            "report_exists",
            (decision_dir / "forward_outcome_decision_report.md").exists(),
            decision_id,
        ),
        _check(
            "reader_brief_exists",
            (decision_dir / "reader_brief_section.md").exists(),
            decision_id,
        ),
        _check("decision_id_matches", manifest.get("decision_id") == decision_id, decision_id),
        _check(
            "source_snapshot_valid",
            not snapshot_errors,
            ",".join(snapshot_errors),
        ),
        _check("views_recomputed", not recompute_error, recompute_error),
        _check("matrix_recomputed", matrix == expected_matrix, "snapshot"),
        _check("actions_recomputed", next_actions == expected_actions, "snapshot"),
        _check("manifest_recomputed", manifest == expected_manifest, "snapshot"),
        _check(
            "report_recomputed",
            actual_report == expected_report,
            "Markdown",
        ),
        _check(
            "reader_brief_recomputed",
            actual_reader == expected_reader,
            "Reader Brief",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and matrix.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action forbidden",
        ),
        _check(
            "production_effect_none",
            manifest.get("production_effect") == "none"
            and matrix.get("production_effect") == "none",
            "production effect",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_forward_outcome_decision_validation",
        artifact_id_key="decision_id",
        artifact_id=decision_id,
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


def render_outcome_update_review_report(
    manifest: Mapping[str, Any],
    safety: Mapping[str, Any],
    impact: Mapping[str, Any],
) -> str:
    return (
        "\n".join(
            [
                "# Outcome Update Ready Review",
                "",
                f"- update_review_id: `{manifest.get('update_review_id')}`",
                f"- due_id: `{manifest.get('due_id')}`",
                f"- status: {manifest.get('status')}",
                f"- ready_to_update_count: {safety.get('ready_to_update_count')}",
                f"- blocked_count: {safety.get('blocked_count')}",
                f"- price_data_available: {safety.get('price_data_available')}",
                f"- future_data_used_in_decision: {safety.get('future_data_used_in_decision')}",
                (
                    "- expected_forward_available_delta: "
                    f"{impact.get('expected_forward_available_delta')}"
                ),
                f"- affected_artifacts: {', '.join(_texts(impact.get('affected_artifacts')))}",
                f"- outcome_update_recommended: {_outcome_update_recommended(safety)}",
                "",
                "本审核包只给出人工复核输入，不触发 outcome update、broker 或 production action。",
            ]
        )
        + "\n"
    )


def render_outcome_update_report(
    manifest: Mapping[str, Any],
    delta: Mapping[str, Any],
    updated: Sequence[Mapping[str, Any]],
    skipped: Sequence[Mapping[str, Any]],
) -> str:
    before = _mapping(delta.get("before"))
    after = _mapping(delta.get("after"))
    return (
        "\n".join(
            [
                "# Safe Outcome Update",
                "",
                f"- outcome_update_id: `{manifest.get('outcome_update_id')}`",
                f"- update_review_id: `{manifest.get('update_review_id')}`",
                f"- status: {manifest.get('status')}",
                f"- transaction_status: {manifest.get('transaction_status')}",
                f"- cohort: {delta.get('cohort')}",
                f"- updated_count: {len(updated)}",
                f"- skipped_count: {len(skipped)}",
                (
                    "- forward_available before / after: "
                    f"{before.get('forward_available')} / {after.get('forward_available')}"
                ),
                (
                    "- forward_pending before / after: "
                    f"{before.get('forward_pending')} / {after.get('forward_pending')}"
                ),
                f"- future_data_used_in_decision: {manifest.get('future_data_used_in_decision')}",
                f"- downstream_refresh_required: {manifest.get('downstream_refresh_required')}",
                (
                    "- automatic_downstream_refresh_allowed: "
                    f"{manifest.get('automatic_downstream_refresh_allowed')}"
                ),
                "",
                "本次更新只处理 review_status=READY_TO_UPDATE 的窗口，并写入 audit artifact。",
            ]
        )
        + "\n"
    )


def _outcome_update_recommended(safety: Mapping[str, Any]) -> bool:
    return _int(safety.get("ready_to_update_count")) > 0 and _int(safety.get("blocked_count")) == 0


def render_rolling_evidence_refresh_report(
    manifest: Mapping[str, Any],
    refreshed: Mapping[str, Any],
    delta: Mapping[str, Any],
) -> str:
    before = _mapping(delta.get("before"))
    after = _mapping(delta.get("after"))
    return (
        "\n".join(
            [
                "# Rolling Evidence Refresh",
                "",
                f"- refresh_id: `{manifest.get('refresh_id')}`",
                f"- outcome_update_id: `{manifest.get('outcome_update_id')}`",
                f"- outcome_dashboard_id: `{refreshed.get('outcome_dashboard_id')}`",
                f"- limited_vs_notrade_id: `{refreshed.get('limited_vs_notrade_id')}`",
                f"- consensus_risk_id: `{refreshed.get('consensus_risk_id')}`",
                f"- owner_attribution_id: `{refreshed.get('owner_attribution_id')}`",
                f"- shadow_aging_id: `{refreshed.get('shadow_aging_id')}`",
                f"- weekly_advisory_review_id: `{refreshed.get('weekly_advisory_review_id')}`",
                (
                    "- forward_available before / after: "
                    f"{before.get('forward_available')} / {after.get('forward_available')}"
                ),
                (
                    "- limited_vs_notrade_available_count before / after: "
                    f"{before.get('limited_vs_notrade_available_count')} / "
                    f"{after.get('limited_vs_notrade_available_count')}"
                ),
                (
                    "- consensus_target_risk before / after: "
                    f"{before.get('consensus_target_risk')} / {after.get('consensus_target_risk')}"
                ),
                f"- material_change: {delta.get('material_change')}",
                f"- recommended_action: {after.get('recommended_action')}",
                "",
                "刷新只更新 evidence artifacts；不修改 advisory policy、不触发 broker。",
            ]
        )
        + "\n"
    )


def render_rolling_refresh_reader_brief(
    manifest: Mapping[str, Any], delta: Mapping[str, Any]
) -> str:
    after = _mapping(delta.get("after"))
    return (
        "\n".join(
            [
                "## Dynamic Rescue Rolling Evidence Refresh",
                "",
                f"- refresh_id: {manifest.get('refresh_id')}",
                f"- forward_available: {after.get('forward_available')}",
                f"- forward_pending: {after.get('forward_pending')}",
                (f"- limited_vs_notrade_confidence: {after.get('limited_vs_notrade_confidence')}"),
                f"- consensus_target_risk: {after.get('consensus_target_risk')}",
                f"- material_change: {delta.get('material_change')}",
                f"- recommended_action: {after.get('recommended_action')}",
            ]
        )
        + "\n"
    )


def render_evidence_trend_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    timeseries: Sequence[Mapping[str, Any]],
) -> str:
    latest = timeseries[-1] if timeseries else {}
    return (
        "\n".join(
            [
                "# Advisory Evidence Trend",
                "",
                f"- trend_id: `{manifest.get('trend_id')}`",
                f"- refresh_count: {len(timeseries)}",
                f"- trend_status: {summary.get('trend_status')}",
                f"- available_sample_growth: {summary.get('available_sample_growth')}",
                f"- confidence_change: {summary.get('confidence_change')}",
                f"- limited_vs_notrade_signal: {summary.get('limited_vs_notrade_signal')}",
                f"- consensus_risk_signal: {summary.get('consensus_risk_signal')}",
                f"- next_action: {summary.get('next_action')}",
                f"- latest_recommended_action: {latest.get('recommended_action', 'MISSING')}",
                "",
                "历史 refresh 不足时，本报告只支持 continue_tracking，不支持规则调整。",
            ]
        )
        + "\n"
    )


def render_forward_outcome_decision_report(
    manifest: Mapping[str, Any],
    matrix: Mapping[str, Any],
    next_actions: Mapping[str, Any],
) -> str:
    action_lines = [
        (
            f"- {row.get('priority')}: {row.get('action')} - "
            f"{row.get('reason', row.get('target_date', ''))}"
        )
        for row in _records(next_actions.get("next_actions"))
    ]
    return (
        "\n".join(
            [
                "# Weekly Forward Outcome Decision Pack",
                "",
                f"- decision_id: `{manifest.get('decision_id')}`",
                f"- week_ending: {matrix.get('week_ending')}",
                f"- evidence_status: {matrix.get('evidence_status')}",
                f"- outcome_update_id: {matrix.get('outcome_update_id')}",
                f"- rolling_refresh_id: {matrix.get('rolling_refresh_id')}",
                f"- evidence_trend_id: {matrix.get('evidence_trend_id')}",
                f"- outcome_update_status: {matrix.get('outcome_update_status')}",
                f"- forward_available: {matrix.get('forward_available')}",
                f"- forward_pending: {matrix.get('forward_pending')}",
                f"- limited_vs_notrade_confidence: {matrix.get('limited_vs_notrade_confidence')}",
                f"- consensus_target_risk: {matrix.get('consensus_target_risk')}",
                f"- evidence_trend_status: {matrix.get('evidence_trend_status')}",
                f"- rule_calibration_readiness: {matrix.get('rule_calibration_readiness')}",
                f"- recommended_action: {matrix.get('recommended_action')}",
                f"- policy_id: {manifest.get('policy_id')}",
                f"- policy_version: {manifest.get('policy_version')}",
                f"- broker_action_allowed: {matrix.get('broker_action_allowed')}",
                f"- production_effect: {matrix.get('production_effect')}",
                "",
                "## Next Actions",
                "",
                *action_lines,
                "",
                "证据不足时不得自动调整 advisory 规则；本包不触发 broker 或 production action。",
            ]
        )
        + "\n"
    )


def render_forward_decision_reader_brief(
    matrix: Mapping[str, Any], next_actions: Mapping[str, Any]
) -> str:
    actions = _records(next_actions.get("next_actions"))
    next_due = ""
    for action in actions:
        if action.get("action") == "run_next_due_scan":
            next_due = _text(action.get("target_date"))
            break
    return (
        "\n".join(
            [
                "## Dynamic Rescue Forward Outcome Decision",
                "",
                f"- week_ending: {matrix.get('week_ending')}",
                f"- evidence_status: {matrix.get('evidence_status')}",
                f"- forward_available: {matrix.get('forward_available')}",
                f"- forward_pending: {matrix.get('forward_pending')}",
                f"- limited_vs_notrade_confidence: {matrix.get('limited_vs_notrade_confidence')}",
                f"- consensus_target_risk: {matrix.get('consensus_target_risk')}",
                f"- recommended_action: {matrix.get('recommended_action')}",
                f"- next_due_scan_date: {next_due}",
            ]
        )
        + "\n"
    )


def _build_outcome_update_review_snapshot(
    *, due_id: str, outcome_due_dir: Path, generated: datetime
) -> dict[str, Any]:
    due_dir = outcome_due_dir / due_id
    if (
        validate_outcome_due_artifact(due_id=due_id, output_dir=outcome_due_dir).get("status")
        != "PASS"
    ):
        raise DynamicV3OutcomeAccumulationError(f"outcome due validation must PASS: {due_id}")
    manifest = _read_json(due_dir / "outcome_due_manifest.json")
    source_generated = _datetime_from_any(manifest.get("generated_at"))
    as_of = _date_from_any(manifest.get("as_of"))
    if (
        manifest.get("due_id") != due_id
        or source_generated is None
        or source_generated > generated
        or as_of is None
        or as_of > generated.date()
    ):
        raise DynamicV3OutcomeAccumulationError("outcome due identity/time exceeds review cutoff")
    return {
        "schema_version": OUTCOME_UPDATE_REVIEW_SNAPSHOT_SCHEMA_VERSION,
        "due_id": due_id,
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "generated_cutoff": generated.isoformat(),
        "outcome_due_dir": str(outcome_due_dir),
        "due_source_validation_status": "PASS",
        "due_source_bundle": _immutable_source_bundle(due_dir),
        "production_effect": "none",
    }


def _outcome_update_review_views(
    snapshot: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any], str]:
    generated = _datetime_from_any(snapshot.get("generated_cutoff"))
    as_of = _date_from_any(snapshot.get("as_of"))
    bundle = _mapping(snapshot.get("due_source_bundle"))
    manifest = _mapping(_source_bundle_content(bundle, "outcome_due_manifest.json"))
    source_rows = _records(_source_bundle_content(bundle, "due_window_inventory.jsonl"))
    if (
        generated is None
        or as_of is None
        or as_of > generated.date()
        or manifest.get("due_id") != snapshot.get("due_id")
        or _text(manifest.get("as_of")) != as_of.isoformat()
    ):
        raise DynamicV3OutcomeAccumulationError("outcome update review snapshot identity invalid")
    identities: set[tuple[str, int]] = set()
    review_rows: list[dict[str, Any]] = []
    for row in source_rows:
        identity = (_text(row.get("outcome_id")), _int(row.get("window_days")))
        due_status = _text(row.get("due_status"))
        current_status = _text(row.get("current_outcome_status"))
        latest_price = _date_from_any(row.get("latest_price_date"))
        if (
            not identity[0]
            or identity[1] not in OUTCOME_WINDOWS
            or identity in identities
            or due_status not in OUTCOME_DUE_STATUSES
            or current_status not in OUTCOME_WINDOW_STATUSES
            or (latest_price is not None and latest_price > as_of)
        ):
            raise DynamicV3OutcomeAccumulationError(
                "outcome update review source row identity/status/time invalid"
            )
        identities.add(identity)
        review_rows.append(_outcome_update_review_row(row))
    review_rows.sort(
        key=lambda row: (
            _text(row.get("window_start")),
            _text(row.get("outcome_id")),
            _int(row.get("window_days")),
        )
    )
    safety = _outcome_update_safety_checks(review_rows)
    impact = _outcome_update_impact_preview(review_rows)
    status = "PASS"
    if not review_rows:
        status = "INSUFFICIENT_DATA"
    elif safety["blocked_count"]:
        status = "PASS_WITH_WARNINGS" if safety["ready_to_update_count"] else "BLOCKED"
    elif not safety["ready_to_update_count"]:
        status = "INSUFFICIENT_DATA"
    return review_rows, safety, impact, status


def _outcome_update_review_manifest(
    *,
    review_dir: Path,
    due_id: str,
    as_of: str,
    generated_at: str,
    status: str,
    rows: Sequence[Mapping[str, Any]],
    safety: Mapping[str, Any],
    source_snapshot_checksum: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_outcome_update_review_manifest",
        "update_review_id": review_dir.name,
        "due_id": due_id,
        "as_of": as_of,
        "generated_at": generated_at,
        "status": status,
        "ready_to_update_count": safety.get("ready_to_update_count"),
        "blocked_count": safety.get("blocked_count"),
        "price_missing_count": sum(
            1 for row in rows if row.get("price_data_available") is False
        ),
        "future_data_used_in_decision": False,
        "source_snapshot_path": str(review_dir / "outcome_update_review_source_snapshot.json"),
        "source_snapshot_checksum": source_snapshot_checksum,
        "outcome_update_review_manifest_path": str(
            review_dir / "outcome_update_review_manifest.json"
        ),
        "update_ready_review_matrix_path": str(review_dir / "update_ready_review_matrix.jsonl"),
        "update_impact_preview_path": str(review_dir / "update_impact_preview.json"),
        "update_safety_checks_path": str(review_dir / "update_safety_checks.json"),
        "outcome_update_review_report_path": str(review_dir / "outcome_update_review_report.md"),
        "requires_owner_review": True,
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _outcome_update_review_row(row: Mapping[str, Any]) -> dict[str, Any]:
    due_status = _text(row.get("due_status"), "INSUFFICIENT_DATA")
    existing_status = _text(row.get("current_outcome_status"), "INSUFFICIENT_DATA")
    can_update = row.get("can_update") is True
    price_data_available = due_status != "PRICE_MISSING" and bool(row.get("latest_price_date"))
    review_reasons: list[str] = []
    review_status = "NEEDS_REVIEW"
    expected_new_status = existing_status
    if due_status == "DUE" and can_update and price_data_available and existing_status == "PENDING":
        review_status = "READY_TO_UPDATE"
        expected_new_status = "AVAILABLE"
    elif due_status in {"NOT_DUE", "PRICE_MISSING", "ALREADY_AVAILABLE"}:
        review_status = "BLOCKED"
        review_reasons.append(due_status)
    else:
        review_reasons.append(due_status or "INSUFFICIENT_DATA")
    if not price_data_available:
        review_reasons.append("price_data_unavailable")
    return {
        "daily_advisory_id": _text(row.get("daily_advisory_id")),
        "outcome_id": _text(row.get("outcome_id")),
        "window_days": _int(row.get("window_days")),
        "window_start": _text(row.get("window_start") or row.get("as_of")),
        "expected_window_end": _text(row.get("expected_window_end")),
        "latest_price_date": _text(row.get("latest_price_date")),
        "due_status": due_status,
        "can_update": can_update,
        "price_data_available": price_data_available,
        "future_data_used_in_decision": False,
        "existing_status": existing_status,
        "expected_new_status": expected_new_status,
        "review_status": review_status,
        "review_reasons": sorted(set(review_reasons)),
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _outcome_update_safety_checks(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    ready = [row for row in rows if row.get("review_status") == "READY_TO_UPDATE"]
    blocked = [
        row
        for row in rows
        if row.get("review_status") == "BLOCKED"
        and row.get("due_status") not in {"NOT_DUE", "ALREADY_AVAILABLE"}
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_outcome_update_safety_checks",
        "future_data_used_in_decision": False,
        "price_data_available": all(row.get("price_data_available") is not False for row in ready),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
        "all_ready_windows_reviewed": all(
            row.get("review_status") in OUTCOME_UPDATE_REVIEW_STATUSES for row in ready
        ),
        "blocked_count": len(blocked),
        "ready_to_update_count": len(ready),
    }


def _outcome_update_impact_preview(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    ready_count = sum(1 for row in rows if row.get("review_status") == "READY_TO_UPDATE")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_outcome_update_impact_preview",
        "expected_forward_available_delta": ready_count,
        "expected_forward_pending_delta": -ready_count,
        "affected_artifacts": [
            "advisory_outcome",
            "outcome_dashboard",
            "limited_vs_notrade",
            "consensus_risk",
            "weekly_advisory_review",
            "reader_brief",
        ],
        "requires_owner_review": True,
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _validated_outcome_update_bundles(
    *, outcome_ids: Sequence[str], advisory_outcome_dir: Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    bundles: dict[str, Any] = {}
    validations: dict[str, Any] = {}
    for outcome_id in outcome_ids:
        outcome_dir = _safe_child_dir(advisory_outcome_dir, outcome_id, "outcome_id")
        validation = validate_advisory_outcome_artifact(
            outcome_id=outcome_id,
            output_dir=advisory_outcome_dir,
        )
        if validation.get("status") != "PASS":
            raise DynamicV3OutcomeAccumulationError(
                f"advisory outcome validation must PASS: {outcome_id}="
                f"{validation.get('status')}"
            )
        bundles[outcome_id] = _immutable_source_bundle(outcome_dir)
        validations[outcome_id] = validation
    return bundles, validations


def _preflight_outcome_update_batch(
    *,
    outcome_ids: Sequence[str],
    ready_keys: set[tuple[str, int]],
    advisory_outcome_dir: Path,
    paper_portfolio_dir: Path,
    prices_path: Path,
    rates_path: Path,
    as_of: date,
    generated: datetime,
) -> None:
    if not outcome_ids:
        return
    with tempfile.TemporaryDirectory(prefix="aits_outcome_update_preflight_") as temporary:
        preflight_root = Path(temporary) / "advisory_outcome"
        preflight_root.mkdir(parents=True, exist_ok=True)
        for outcome_id in outcome_ids:
            source = _safe_child_dir(advisory_outcome_dir, outcome_id, "outcome_id")
            copied = preflight_root / outcome_id
            shutil.copytree(source, copied)
            _rebase_outcome_bundle_paths(copied)
        for outcome_id in outcome_ids:
            update_advisory_outcome(
                as_of=as_of,
                outcome_id=outcome_id,
                output_dir=preflight_root,
                paper_portfolio_dir=paper_portfolio_dir,
                prices_path=prices_path,
                rates_path=rates_path,
                generated_at=generated,
                allowed_window_days={
                    window_days
                    for selected_outcome_id, window_days in ready_keys
                    if selected_outcome_id == outcome_id
                },
                update_latest_pointer=False,
            )
            validation = validate_advisory_outcome_artifact(
                outcome_id=outcome_id,
                output_dir=preflight_root,
            )
            if validation.get("status") != "PASS":
                raise DynamicV3OutcomeAccumulationError(
                    f"outcome update isolated preflight failed: {outcome_id}="
                    f"{validation.get('status')}"
                )


def _rebase_outcome_bundle_paths(outcome_dir: Path) -> None:
    manifest_path = outcome_dir / "advisory_outcome_manifest.json"
    manifest = _read_json(manifest_path)
    manifest.update(
        {
            "advisory_event_path": str(outcome_dir / "advisory_event.json"),
            "outcome_windows_path": str(outcome_dir / "outcome_windows.jsonl"),
            "outcome_update_events_path": str(outcome_dir / "outcome_update_events.jsonl"),
            "advisory_counterfactuals_path": str(outcome_dir / "advisory_counterfactuals.json"),
            "advisory_outcome_report_path": str(outcome_dir / "advisory_outcome_report.md"),
            "source_snapshots_root": str(outcome_dir / "source_snapshots"),
        }
    )
    _write_json(manifest_path, manifest)


def _backup_outcome_update_sources(
    *, outcome_ids: Sequence[str], advisory_outcome_dir: Path, backup_root: Path
) -> dict[str, str]:
    paths: dict[str, str] = {}
    for outcome_id in outcome_ids:
        source = _safe_child_dir(advisory_outcome_dir, outcome_id, "outcome_id")
        destination = backup_root / outcome_id
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source, destination)
        paths[outcome_id] = str(destination)
    return paths


def _restore_outcome_update_sources(
    *, outcome_ids: Sequence[str], advisory_outcome_dir: Path, backup_root: Path
) -> dict[str, str]:
    statuses: dict[str, str] = {}
    for outcome_id in outcome_ids:
        backup = _safe_child_dir(backup_root, outcome_id, "outcome_id")
        destination = advisory_outcome_dir / outcome_id
        if not backup.is_dir():
            raise DynamicV3OutcomeAccumulationError(
                f"outcome update rollback backup missing: {backup}"
            )
        if destination.exists():
            shutil.rmtree(destination)
        shutil.copytree(backup, destination)
        validation = validate_advisory_outcome_artifact(
            outcome_id=outcome_id,
            output_dir=advisory_outcome_dir,
        )
        status = _text(validation.get("status"))
        statuses[outcome_id] = status
        if status != "PASS":
            raise DynamicV3OutcomeAccumulationError(
                f"outcome update rollback validation failed: {outcome_id}={status}"
            )
    if backup_root.exists():
        shutil.rmtree(backup_root)
    return statuses


def _outcome_update_transaction(
    *,
    update_id: str,
    update_review_id: str,
    generated: datetime,
    status: str,
    selected_outcome_ids: Sequence[str],
    ready_outcome_ids: Sequence[str],
    backup_paths: Mapping[str, str],
    error: str = "",
    rollback_validation: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    if status not in {"PREPARED", "COMMITTED", "ROLLED_BACK"}:
        raise DynamicV3OutcomeAccumulationError("outcome update transaction status invalid")
    return {
        "schema_version": OUTCOME_UPDATE_TRANSACTION_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_outcome_update_transaction",
        "outcome_update_id": update_id,
        "update_review_id": update_review_id,
        "transaction_at": generated.isoformat(),
        "status": status,
        "selected_outcome_ids": list(selected_outcome_ids),
        "ready_outcome_ids": list(ready_outcome_ids),
        "rollback_backup_paths": dict(backup_paths),
        "rollback_required": status == "PREPARED",
        "rollback_validation": dict(rollback_validation or {}),
        "error": error,
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _recover_prepared_outcome_updates(
    *, output_dir: Path, update_review_id: str, advisory_outcome_dir: Path
) -> None:
    if not output_dir.exists():
        return
    for child in sorted(item for item in output_dir.iterdir() if item.is_dir()):
        transaction_path = child / "outcome_update_transaction.json"
        transaction = _read_optional_json(transaction_path) or {}
        if (
            transaction.get("update_review_id") != update_review_id
            or transaction.get("status") != "PREPARED"
        ):
            continue
        ready_ids = _texts(transaction.get("ready_outcome_ids"))
        generated = _datetime_from_any(transaction.get("transaction_at"))
        if generated is None:
            raise DynamicV3OutcomeAccumulationError(
                f"prepared outcome update transaction time invalid: {child.name}"
            )
        rollback_validation = _restore_outcome_update_sources(
            outcome_ids=ready_ids,
            advisory_outcome_dir=advisory_outcome_dir,
            backup_root=child / "rollback_backups",
        )
        recovered = _outcome_update_transaction(
            update_id=child.name,
            update_review_id=update_review_id,
            generated=generated,
            status="ROLLED_BACK",
            selected_outcome_ids=_texts(transaction.get("selected_outcome_ids")),
            ready_outcome_ids=ready_ids,
            backup_paths={},
            error="recovered_prepared_transaction_before_retry",
            rollback_validation=rollback_validation,
        )
        _write_json(transaction_path, recovered)


def _committed_outcome_update_for_review(output_dir: Path, update_review_id: str) -> bool:
    if not output_dir.exists():
        return False
    committed = []
    for child in sorted(item for item in output_dir.iterdir() if item.is_dir()):
        transaction = _read_optional_json(child / "outcome_update_transaction.json") or {}
        if (
            transaction.get("update_review_id") == update_review_id
            and transaction.get("status") == "COMMITTED"
        ):
            committed.append(child.name)
    if len(committed) > 1:
        raise DynamicV3OutcomeAccumulationError(
            "multiple COMMITTED outcome updates found for one review"
        )
    return bool(committed)


def _outcome_rows_by_window_from_bundles(
    bundles: Mapping[str, Any],
) -> dict[tuple[str, int], dict[str, Any]]:
    rows: dict[tuple[str, int], dict[str, Any]] = {}
    for outcome_id, raw_bundle in sorted(bundles.items()):
        bundle = _mapping(raw_bundle)
        manifest = _mapping(_source_bundle_content(bundle, "advisory_outcome_manifest.json"))
        for raw_row in _records(_source_bundle_content(bundle, "outcome_windows.jsonl")):
            row = {
                **raw_row,
                "outcome_id": outcome_id,
                "daily_advisory_id": _text(manifest.get("daily_advisory_id")),
            }
            key = (outcome_id, _int(row.get("window_days")))
            if not key[0] or key[1] <= 0 or key in rows:
                raise DynamicV3OutcomeAccumulationError(
                    "outcome update bundle contains duplicate or invalid window identity"
                )
            rows[key] = row
    return rows


def _outcome_update_derived_views(
    snapshot: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    review_bundle = _mapping(snapshot.get("review_source_bundle"))
    review_rows = _records(
        _source_bundle_content(review_bundle, "update_ready_review_matrix.jsonl")
    )
    before_by_window = _outcome_rows_by_window_from_bundles(
        _mapping(snapshot.get("pre_outcome_bundles"))
    )
    after_by_window = _outcome_rows_by_window_from_bundles(
        _mapping(snapshot.get("post_outcome_bundles"))
    )
    updated_windows: list[dict[str, Any]] = []
    skipped_windows: list[dict[str, Any]] = []
    for row in review_rows:
        outcome_id = _text(row.get("outcome_id"))
        window_days = _int(row.get("window_days"))
        key = (outcome_id, window_days)
        before = before_by_window.get(key, {})
        after = after_by_window.get(key, before)
        if row.get("review_status") != "READY_TO_UPDATE":
            skipped_windows.append(_skipped_outcome_update_row(row, before))
        elif _text(after.get("outcome_status")) == "AVAILABLE":
            updated_windows.append(_updated_outcome_window_row(row, before, after))
        else:
            skipped_windows.append(
                {
                    "daily_advisory_id": _text(row.get("daily_advisory_id")),
                    "outcome_id": outcome_id,
                    "window_days": window_days,
                    "old_status": _text(before.get("outcome_status"), "PENDING"),
                    "skip_reason": "INSUFFICIENT_DATA",
                    "review_status": "READY_TO_UPDATE",
                    "new_status": _text(after.get("outcome_status"), "INSUFFICIENT_DATA"),
                    "broker_action_taken": False,
                }
            )
    before_rows = list(before_by_window.values())
    after_rows = list(after_by_window.values())
    delta = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_outcome_status_delta",
        "cohort": "selected_review_outcomes",
        "before": _forward_status_summary(before_rows),
        "after": _forward_status_summary(after_rows),
        "updated_count": len(updated_windows),
        "skipped_count": len(skipped_windows),
        "production_effect": "none",
        "broker_action_taken": False,
    }
    return updated_windows, skipped_windows, delta


def _outcome_update_manifest(
    *,
    update_dir: Path,
    update_review_id: str,
    review_manifest: Mapping[str, Any],
    generated: datetime,
    as_of: date,
    updated_windows: Sequence[Mapping[str, Any]],
    skipped_windows: Sequence[Mapping[str, Any]],
    source_snapshot_checksum: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_outcome_update_manifest",
        "outcome_update_id": update_dir.name,
        "update_review_id": update_review_id,
        "due_id": _text(review_manifest.get("due_id")),
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS" if updated_windows else "INSUFFICIENT_DATA",
        "transaction_status": "COMMITTED",
        "updated_count": len(updated_windows),
        "skipped_count": len(skipped_windows),
        "future_data_used_in_decision": False,
        "downstream_refresh_required": bool(updated_windows),
        "automatic_downstream_refresh_allowed": False,
        "source_snapshot_path": str(update_dir / "outcome_update_source_snapshot.json"),
        "source_snapshot_checksum": source_snapshot_checksum,
        "transaction_path": str(update_dir / "outcome_update_transaction.json"),
        "outcome_update_manifest_path": str(update_dir / "outcome_update_manifest.json"),
        "updated_windows_path": str(update_dir / "updated_windows.jsonl"),
        "skipped_windows_path": str(update_dir / "skipped_windows.jsonl"),
        "outcome_status_delta_path": str(update_dir / "outcome_status_delta.json"),
        "outcome_update_report_path": str(update_dir / "outcome_update_report.md"),
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _outcome_update_snapshot_errors(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    generated = _datetime_from_any(snapshot.get("generated_at"))
    review_generated = _datetime_from_any(snapshot.get("review_generated_at"))
    as_of = _date_from_any(snapshot.get("as_of"))
    selected_ids = _texts(snapshot.get("selected_outcome_ids"))
    ready_ids = _texts(snapshot.get("ready_outcome_ids"))
    if (
        generated is None
        or review_generated is None
        or as_of is None
        or generated < review_generated
        or as_of > generated.date()
        or snapshot.get("future_data_used_in_decision") is not False
        or snapshot.get("transaction_status") != "COMMITTED"
    ):
        errors.append("time_or_transaction_boundary_invalid")
    if (
        len(selected_ids) != len(set(selected_ids))
        or len(ready_ids) != len(set(ready_ids))
        or not set(ready_ids) <= set(selected_ids)
    ):
        errors.append("selected_identity_invalid")
    review_bundle = _mapping(snapshot.get("review_source_bundle"))
    if not _source_bundle_matches(review_bundle):
        errors.append("review_source_bundle_changed")
    review_snapshot = _mapping(
        _source_bundle_content(review_bundle, "outcome_update_review_source_snapshot.json")
    )
    try:
        expected_review_rows, expected_safety, expected_impact, expected_status = (
            _outcome_update_review_views(review_snapshot)
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(f"review_snapshot_invalid:{exc}")
    else:
        review_manifest = _mapping(
            _source_bundle_content(review_bundle, "outcome_update_review_manifest.json")
        )
        review_rows = _records(
            _source_bundle_content(review_bundle, "update_ready_review_matrix.jsonl")
        )
        safety = _mapping(_source_bundle_content(review_bundle, "update_safety_checks.json"))
        impact = _mapping(_source_bundle_content(review_bundle, "update_impact_preview.json"))
        report_content = _source_bundle_content(
            review_bundle, "outcome_update_review_report.md"
        )
        report = report_content if isinstance(report_content, str) else ""
        expected_manifest = _outcome_update_review_manifest(
            review_dir=Path(_text(review_bundle.get("root"))),
            due_id=_text(review_snapshot.get("due_id")),
            as_of=_text(review_snapshot.get("as_of")),
            generated_at=_text(review_snapshot.get("generated_at")),
            status=expected_status,
            rows=expected_review_rows,
            safety=expected_safety,
            source_snapshot_checksum=_text(
                _mapping(
                    _mapping(review_bundle.get("files")).get(
                        "outcome_update_review_source_snapshot.json"
                    )
                ).get("sha256")
            ),
        )
        expected_report = render_outcome_update_review_report(
            expected_manifest, expected_safety, expected_impact
        )
        if (
            review_rows != expected_review_rows
            or safety != expected_safety
            or impact != expected_impact
            or review_manifest != expected_manifest
            or report != expected_report
        ):
            errors.append("review_bundle_recompute_mismatch")
    pre = _mapping(snapshot.get("pre_outcome_bundles"))
    post = _mapping(snapshot.get("post_outcome_bundles"))
    pre_validations = _mapping(snapshot.get("pre_outcome_validations"))
    post_validations = _mapping(snapshot.get("post_outcome_validations"))
    if set(pre) != set(selected_ids) or set(post) != set(selected_ids):
        errors.append("pre_post_bundle_identity_mismatch")
    if any(_mapping(value).get("status") != "PASS" for value in pre_validations.values()):
        errors.append("pre_validation_not_pass")
    if any(_mapping(value).get("status") != "PASS" for value in post_validations.values()):
        errors.append("post_validation_not_pass")
    ready_keys = {
        (_text(item[0]), _int(item[1]))
        for item in _records_or_lists(snapshot.get("ready_keys"))
        if len(item) == 2
    }
    for outcome_id in selected_ids:
        pre_events = _records(
            _source_bundle_content(_mapping(pre.get(outcome_id)), "outcome_update_events.jsonl")
        )
        post_events = _records(
            _source_bundle_content(_mapping(post.get(outcome_id)), "outcome_update_events.jsonl")
        )
        if outcome_id in ready_ids:
            expected_windows = sorted(
                window for selected_id, window in ready_keys if selected_id == outcome_id
            )
            if (
                len(post_events) != len(pre_events) + 1
                or post_events[:-1] != pre_events
                or sorted(
                    _int(value)
                    for value in post_events[-1].get("allowed_window_days", [])
                )
                != expected_windows
                or _date_from_any(post_events[-1].get("updated_as_of")) != as_of
                or _datetime_from_any(post_events[-1].get("event_at")) != generated
            ):
                errors.append(f"post_event_append_invalid:{outcome_id}")
        elif post_events != pre_events:
            errors.append(f"non_ready_outcome_mutated:{outcome_id}")
    return sorted(set(errors))


def _records_or_lists(value: Any) -> list[list[Any]]:
    return [list(item) for item in value] if isinstance(value, list) else []


def _safe_child_dir(root: Path, identifier: str, field: str) -> Path:
    if (
        not identifier
        or identifier in {".", ".."}
        or Path(identifier).name != identifier
        or "/" in identifier
        or "\\" in identifier
    ):
        raise DynamicV3OutcomeAccumulationError(f"{field} must be a safe artifact id")
    child = root / identifier
    if child.resolve().parent != root.resolve():
        raise DynamicV3OutcomeAccumulationError(f"{field} escapes artifact root")
    return child


def _forward_rows_by_outcome_window(
    rows: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, int], dict[str, Any]]:
    return {(_text(row.get("outcome_id")), _int(row.get("window_days"))): dict(row) for row in rows}


def _forward_status_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts = _status_counts(rows)
    return {
        "forward_available": counts["available"],
        "forward_pending": counts["pending"],
        "forward_insufficient": counts["insufficient_data"],
    }


def _updated_outcome_window_row(
    review: Mapping[str, Any], before: Mapping[str, Any], after: Mapping[str, Any]
) -> dict[str, Any]:
    return {
        "daily_advisory_id": _text(review.get("daily_advisory_id")),
        "outcome_id": _text(review.get("outcome_id")),
        "window_days": _int(review.get("window_days")),
        "old_status": _text(before.get("outcome_status"), "PENDING"),
        "new_status": _text(after.get("outcome_status"), "AVAILABLE"),
        "return": _float(after.get("paper_portfolio_return")),
        "relative_to_no_trade": _float(after.get("relative_to_no_trade")),
        "relative_to_baseline": _float(after.get("relative_to_baseline")),
        "max_drawdown": _float(after.get("max_drawdown")),
        "realized_volatility": _float(after.get("realized_volatility")),
        "price_start_date": _text(after.get("start_date") or review.get("window_start")),
        "price_end_date": _text(after.get("end_date") or review.get("expected_window_end")),
        "future_data_used_in_decision": False,
        "broker_action_taken": False,
    }


def _skipped_outcome_update_row(
    review: Mapping[str, Any], before: Mapping[str, Any]
) -> dict[str, Any]:
    due_status = _text(review.get("due_status"), "INSUFFICIENT_DATA")
    skip_reason = "BLOCKED_BY_REVIEW"
    if due_status in {"NOT_DUE", "PRICE_MISSING", "INSUFFICIENT_DATA"}:
        skip_reason = due_status
    return {
        "daily_advisory_id": _text(review.get("daily_advisory_id")),
        "outcome_id": _text(review.get("outcome_id")),
        "window_days": _int(review.get("window_days")),
        "old_status": _text(before.get("outcome_status"), _text(review.get("existing_status"))),
        "skip_reason": skip_reason,
        "review_status": _text(review.get("review_status")),
        "broker_action_taken": False,
    }


def _rollup_forward_rows_status(rows: Sequence[Mapping[str, Any]]) -> str:
    statuses = {_text(row.get("outcome_status")) for row in rows}
    if statuses == {"AVAILABLE"}:
        return "AVAILABLE"
    if "PENDING" in statuses:
        return "PENDING"
    if "AVAILABLE" in statuses and "INSUFFICIENT_DATA" in statuses:
        return "PARTIAL"
    return "INSUFFICIENT_DATA"


def _latest_limited_vs_notrade_summary(output_dir: Path) -> dict[str, Any]:
    try:
        payload = limited_vs_notrade_report_payload(latest=True, output_dir=output_dir)
    except DynamicV3OutcomeAccumulationError:
        return {
            "available_count": 0,
            "confidence": "INSUFFICIENT_DATA",
            "win_rate": 0.0,
            "avg_relative_return": 0.0,
            "recommendation": "insufficient_data",
        }
    metrics = _records(_mapping(payload.get("window_comparison_metrics")).get("by_window"))
    first = metrics[0] if metrics else {}
    return {
        "available_count": _int(payload.get("available_count")),
        "confidence": _text(first.get("confidence"), "INSUFFICIENT_DATA"),
        "win_rate": _float(first.get("win_rate")),
        "avg_relative_return": _float(first.get("avg_relative_return")),
        "recommendation": _text(
            _mapping(payload.get("window_comparison_metrics")).get("overall_recommendation"),
            "insufficient_data",
        ),
    }


def _latest_consensus_risk_summary(output_dir: Path) -> dict[str, Any]:
    try:
        payload = consensus_risk_report_payload(latest=True, output_dir=output_dir)
    except DynamicV3OutcomeAccumulationError:
        return {"consensus_target_risk": "INSUFFICIENT_DATA"}
    return {
        "consensus_target_risk": _text(payload.get("consensus_target_risk"), "INSUFFICIENT_DATA")
    }


def _rolling_evidence_delta_summary(
    *,
    before_dashboard: Mapping[str, Any],
    before_limited: Mapping[str, Any],
    before_consensus: Mapping[str, Any],
    dashboard: Mapping[str, Any],
    limited: Mapping[str, Any],
    consensus: Mapping[str, Any],
) -> dict[str, Any]:
    matrix = _mapping(_mapping(dashboard.get("outcome_availability_matrix")).get("summary"))
    forward = _mapping(matrix.get("forward_outcome"))
    historical = _mapping(matrix.get("historical_replay"))
    limited_summary = _latest_limited_from_result(limited)
    consensus_status = _text(
        _mapping(consensus.get("manifest")).get("consensus_target_risk"),
        "INSUFFICIENT_DATA",
    )
    before = {
        "forward_available": _int(before_dashboard.get("forward_available")),
        "forward_pending": _int(before_dashboard.get("forward_pending")),
        "limited_vs_notrade_available_count": _int(before_limited.get("available_count")),
        "limited_vs_notrade_confidence": _text(
            before_limited.get("confidence"), "INSUFFICIENT_DATA"
        ),
        "consensus_target_risk": _text(
            before_consensus.get("consensus_target_risk"), "INSUFFICIENT_DATA"
        ),
    }
    after = {
        "forward_available": _int(forward.get("available")),
        "forward_pending": _int(forward.get("pending")),
        "historical_replay_available": _int(historical.get("available")),
        "historical_replay_pending": _int(historical.get("pending")),
        "limited_vs_notrade_available_count": limited_summary["available_count"],
        "limited_vs_notrade_win_rate": limited_summary["win_rate"],
        "limited_vs_notrade_avg_relative_return": limited_summary["avg_relative_return"],
        "limited_vs_notrade_confidence": limited_summary["confidence"],
        "consensus_target_risk": consensus_status,
        "recommended_action": (
            "continue_tracking"
            if limited_summary["confidence"] in {"LOW", "INSUFFICIENT_DATA"}
            else limited_summary["recommendation"]
        ),
    }
    material_reasons = []
    if before["limited_vs_notrade_confidence"] != after["limited_vs_notrade_confidence"]:
        material_reasons.append("limited_vs_notrade_confidence_changed")
    if before["consensus_target_risk"] != after["consensus_target_risk"]:
        material_reasons.append("consensus_target_risk_changed")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_evidence_delta_summary",
        "before": before,
        "after": after,
        "material_change": bool(material_reasons),
        "material_change_reasons": material_reasons,
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _latest_limited_from_result(result: Mapping[str, Any]) -> dict[str, Any]:
    metrics = _records(_mapping(result.get("window_comparison_metrics")).get("by_window"))
    first = metrics[0] if metrics else {}
    return {
        "available_count": _int(_mapping(result.get("manifest")).get("available_count")),
        "confidence": _text(first.get("confidence"), "INSUFFICIENT_DATA"),
        "win_rate": _float(first.get("win_rate")),
        "avg_relative_return": _float(first.get("avg_relative_return")),
        "recommendation": _text(
            _mapping(result.get("window_comparison_metrics")).get("overall_recommendation"),
            "insufficient_data",
        ),
    }


def _resolve_shadow_shortlist_id(shadow_aging_dir: Path) -> str:
    children = _artifact_children(shadow_aging_dir)
    for child in reversed(children):
        manifest = _read_optional_json(child / "shadow_aging_manifest.json") or {}
        shadow_shortlist_id = _text(manifest.get("shadow_shortlist_id"))
        if shadow_shortlist_id:
            return shadow_shortlist_id
    return ""


def _load_evidence_trend_policy(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping) or payload.get("schema_version") != (
        "etf_dynamic_v3_evidence_trend_policy.v1"
    ):
        raise DynamicV3OutcomeAccumulationError("evidence trend policy schema invalid")
    policy = dict(payload)
    required_text = (
        "policy_id",
        "version",
        "status",
        "owner",
        "rationale",
        "review_condition",
        "validation_plan",
    )
    if not all(_text(policy.get(key)) for key in required_text):
        raise DynamicV3OutcomeAccumulationError("evidence trend policy governance incomplete")
    confidence_order = _texts(policy.get("confidence_order"))
    trend_precedence = _texts(policy.get("trend_precedence"))
    next_actions = _mapping(policy.get("next_action_by_trend"))
    growth_policy = _mapping(policy.get("availability_growth"))
    return_policy = _mapping(policy.get("relative_return_signal"))
    risk_statuses = _texts(policy.get("consensus_risk_signal_statuses"))
    manual_review_statuses = _texts(policy.get("manual_review_risk_statuses"))
    try:
        return_floor = float(return_policy.get("early_positive_exclusive_floor"))
    except (TypeError, ValueError):
        return_floor = math.nan
    if (
        _int(policy.get("minimum_history_refresh_count")) < 2
        or len(confidence_order) != len(set(confidence_order))
        or set(confidence_order) != {"INSUFFICIENT_DATA", "LOW", "MEDIUM", "HIGH"}
        or trend_precedence != ["DETERIORATING", "IMPROVING", "STABLE"]
        or _int(growth_policy.get("improving_minimum")) < 1
        or _int(growth_policy.get("deteriorating_maximum")) > -1
        or not math.isfinite(return_floor)
        or not set(_texts(return_policy.get("eligible_confidence"))).issubset(
            set(confidence_order) - {"INSUFFICIENT_DATA"}
        )
        or not risk_statuses
        or len(risk_statuses) != len(set(risk_statuses))
        or not manual_review_statuses
        or len(manual_review_statuses) != len(set(manual_review_statuses))
        or set(next_actions)
        != {"INSUFFICIENT_HISTORY", "DETERIORATING", "IMPROVING", "STABLE"}
        or any(not _text(value) for value in next_actions.values())
        or policy.get("production_effect") != "none"
        or policy.get("broker_action_allowed") is not False
        or policy.get("auto_policy_apply") is not False
    ):
        raise DynamicV3OutcomeAccumulationError("evidence trend policy contract invalid")
    return policy


def _evidence_trend_manifest(
    *,
    trend_dir: Path,
    generated: datetime,
    timeseries: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
    policy: Mapping[str, Any],
    source_snapshot_checksum: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_evidence_trend_manifest",
        "trend_id": trend_dir.name,
        "generated_at": generated.isoformat(),
        "status": "PASS" if timeseries else "INSUFFICIENT_DATA",
        "refresh_count": len(timeseries),
        "trend_status": summary.get("trend_status"),
        "policy_id": policy.get("policy_id"),
        "policy_version": policy.get("version"),
        "source_snapshot_path": str(trend_dir / "evidence_trend_source_snapshot.json"),
        "source_snapshot_checksum": source_snapshot_checksum,
        "evidence_trend_manifest_path": str(trend_dir / "evidence_trend_manifest.json"),
        "evidence_trend_timeseries_path": str(trend_dir / "evidence_trend_timeseries.jsonl"),
        "confidence_trend_summary_path": str(trend_dir / "confidence_trend_summary.json"),
        "evidence_trend_report_path": str(trend_dir / "evidence_trend_report.md"),
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "auto_policy_apply": False,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _build_evidence_trend_snapshot(
    *,
    rolling_refresh_dir: Path,
    policy_path: Path,
    policy: Mapping[str, Any],
    generated: datetime,
) -> dict[str, Any]:
    selected: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    refresh_ids: set[str] = set()
    update_ids: set[str] = set()
    for child in _artifact_children(rolling_refresh_dir):
        transaction = _read_optional_json(child / "rolling_refresh_transaction.json") or {}
        manifest = _read_optional_json(child / "rolling_refresh_manifest.json") or {}
        transaction_status = _text(transaction.get("status"))
        if transaction_status == "ROLLED_BACK":
            excluded.append(
                {
                    "refresh_id": child.name,
                    "reason": "ROLLED_BACK",
                    "source_bundle": _immutable_source_bundle(child),
                    "production_effect": "none",
                }
            )
            continue
        if transaction_status == "PREPARED":
            raise DynamicV3OutcomeAccumulationError(
                f"prepared rolling refresh blocks trend snapshot: {child.name}"
            )
        source_generated = _datetime_from_any(manifest.get("generated_at"))
        if source_generated is not None and source_generated > generated:
            excluded.append(
                {
                    "refresh_id": child.name,
                    "reason": "FUTURE_GENERATED",
                    "source_bundle": _immutable_source_bundle(child),
                    "production_effect": "none",
                }
            )
            continue
        if not manifest or source_generated is None:
            raise DynamicV3OutcomeAccumulationError(
                f"rolling refresh manifest invalid for trend: {child.name}"
            )
        refresh_id = _text(manifest.get("refresh_id"))
        update_id = _text(manifest.get("outcome_update_id"))
        validation = validate_rolling_evidence_refresh_artifact(
            refresh_id=refresh_id,
            output_dir=rolling_refresh_dir,
        )
        if validation.get("status") == "PASS_WITH_WARNINGS":
            excluded.append(
                {
                    "refresh_id": refresh_id or child.name,
                    "reason": "LEGACY_UNSNAPSHOTTED",
                    "source_bundle": _immutable_source_bundle(child),
                    "validation": validation,
                    "production_effect": "none",
                }
            )
            continue
        source_as_of = _date_from_any(manifest.get("as_of"))
        if (
            validation.get("status") != "PASS"
            or transaction_status != "COMMITTED"
            or not refresh_id
            or not update_id
            or refresh_id != child.name
            or source_as_of is None
            or source_as_of > generated.date()
        ):
            raise DynamicV3OutcomeAccumulationError(
                f"committed rolling refresh validation failed: {child.name}="
                f"{validation.get('status')}"
            )
        if refresh_id in refresh_ids or update_id in update_ids:
            raise DynamicV3OutcomeAccumulationError(
                "evidence trend contains duplicate refresh or outcome update identity"
            )
        refresh_ids.add(refresh_id)
        update_ids.add(update_id)
        selected.append(
            {
                "refresh_id": refresh_id,
                "outcome_update_id": update_id,
                "generated_at": source_generated.isoformat(),
                "as_of": source_as_of.isoformat(),
                "source_bundle": _immutable_source_bundle(child),
                "validation": validation,
            }
        )
    selected.sort(
        key=lambda row: (
            _text(row.get("generated_at")),
            _text(row.get("as_of")),
            _text(row.get("refresh_id")),
        )
    )
    return {
        "schema_version": EVIDENCE_TREND_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_evidence_trend_source_snapshot",
        "generated_at": generated.isoformat(),
        "rolling_refresh_dir": str(rolling_refresh_dir),
        "selected_refreshes": selected,
        "excluded_refreshes": excluded,
        "policy_path": str(policy_path),
        "policy_checksum": _file_sha256(policy_path),
        "policy": dict(policy),
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _evidence_trend_snapshot_errors(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    generated = _datetime_from_any(snapshot.get("generated_at"))
    if (
        snapshot.get("schema_version") != EVIDENCE_TREND_SNAPSHOT_SCHEMA_VERSION
        or generated is None
        or snapshot.get("production_effect") != "none"
        or snapshot.get("broker_action_taken") is not False
    ):
        errors.append("snapshot_contract_invalid")
    policy_path = Path(_text(snapshot.get("policy_path")))
    try:
        policy_matches = (
            _file_sha256(policy_path) == snapshot.get("policy_checksum")
            and _load_evidence_trend_policy(policy_path) == snapshot.get("policy")
        )
    except Exception:  # noqa: BLE001
        policy_matches = False
    if not policy_matches:
        errors.append("policy_snapshot_changed")
    refresh_ids: set[str] = set()
    update_ids: set[str] = set()
    previous_order: tuple[str, str, str] | None = None
    for selected in _records(snapshot.get("selected_refreshes")):
        refresh_id = _text(selected.get("refresh_id"))
        update_id = _text(selected.get("outcome_update_id"))
        selected_generated = _datetime_from_any(selected.get("generated_at"))
        as_of = _date_from_any(selected.get("as_of"))
        bundle = _mapping(selected.get("source_bundle"))
        root = Path(_text(bundle.get("root")))
        order = (
            _text(selected.get("generated_at")),
            _text(selected.get("as_of")),
            refresh_id,
        )
        if (
            not refresh_id
            or not update_id
            or refresh_id in refresh_ids
            or update_id in update_ids
            or selected_generated is None
            or generated is None
            or selected_generated > generated
            or as_of is None
            or as_of > generated.date()
            or (previous_order is not None and order < previous_order)
            or root.name != refresh_id
            or not _source_bundle_matches(bundle)
        ):
            errors.append(f"selected_refresh_invalid:{refresh_id or 'missing'}")
            continue
        refresh_ids.add(refresh_id)
        update_ids.add(update_id)
        previous_order = order
        live_validation = validate_rolling_evidence_refresh_artifact(
            refresh_id=refresh_id,
            output_dir=root.parent,
        )
        manifest = _mapping(
            _source_bundle_content(bundle, "rolling_refresh_manifest.json")
        )
        transaction = _mapping(
            _source_bundle_content(bundle, "rolling_refresh_transaction.json")
        )
        if (
            live_validation.get("status") != "PASS"
            or _mapping(selected.get("validation")) != live_validation
            or manifest.get("refresh_id") != refresh_id
            or manifest.get("outcome_update_id") != update_id
            or transaction.get("status") != "COMMITTED"
            or transaction.get("refresh_id") != refresh_id
            or transaction.get("outcome_update_id") != update_id
        ):
            errors.append(f"selected_refresh_live_validation_failed:{refresh_id}")
    allowed_exclusions = {"ROLLED_BACK", "FUTURE_GENERATED", "LEGACY_UNSNAPSHOTTED"}
    excluded_ids: set[str] = set()
    for excluded in _records(snapshot.get("excluded_refreshes")):
        refresh_id = _text(excluded.get("refresh_id"))
        reason = _text(excluded.get("reason"))
        bundle = _mapping(excluded.get("source_bundle"))
        root = Path(_text(bundle.get("root")))
        manifest = _mapping(
            _source_bundle_content(bundle, "rolling_refresh_manifest.json")
        )
        transaction = _mapping(
            _source_bundle_content(bundle, "rolling_refresh_transaction.json")
        )
        if (
            not refresh_id
            or refresh_id in refresh_ids
            or refresh_id in excluded_ids
            or reason not in allowed_exclusions
            or excluded.get("production_effect") != "none"
            or not _source_bundle_matches(bundle)
        ):
            errors.append(f"excluded_refresh_invalid:{refresh_id or 'missing'}")
            continue
        excluded_ids.add(refresh_id)
        if reason == "ROLLED_BACK" and transaction.get("status") != "ROLLED_BACK":
            errors.append(f"excluded_refresh_reason_mismatch:{refresh_id}")
        elif reason == "FUTURE_GENERATED":
            source_generated = _datetime_from_any(manifest.get("generated_at"))
            if generated is None or source_generated is None or source_generated <= generated:
                errors.append(f"excluded_refresh_reason_mismatch:{refresh_id}")
        elif reason == "LEGACY_UNSNAPSHOTTED":
            live_validation = validate_rolling_evidence_refresh_artifact(
                refresh_id=_text(manifest.get("refresh_id")),
                output_dir=root.parent,
            )
            if (
                live_validation.get("status") != "PASS_WITH_WARNINGS"
                or _mapping(excluded.get("validation")) != live_validation
            ):
                errors.append(f"excluded_refresh_reason_mismatch:{refresh_id}")
    return errors


def _evidence_trend_timeseries(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for selected in _records(snapshot.get("selected_refreshes")):
        bundle = _mapping(selected.get("source_bundle"))
        refresh_snapshot = _mapping(
            _source_bundle_content(bundle, "rolling_refresh_source_snapshot.json")
        )
        post = _mapping(refresh_snapshot.get("post_artifacts"))
        dashboard_bundle = _mapping(
            _mapping(post.get("outcome_dashboard")).get("source_bundle")
        )
        limited_bundle = _mapping(
            _mapping(post.get("limited_vs_notrade")).get("source_bundle")
        )
        consensus_bundle = _mapping(
            _mapping(post.get("consensus_risk")).get("source_bundle")
        )
        dashboard_matrix = _mapping(
            _source_bundle_content(dashboard_bundle, "outcome_availability_matrix.json")
        )
        dashboard_summary = _mapping(dashboard_matrix.get("summary"))
        forward = _mapping(dashboard_summary.get("forward_outcome"))
        historical = _mapping(dashboard_summary.get("historical_replay"))
        limited_manifest = _mapping(
            _source_bundle_content(limited_bundle, "limited_vs_notrade_manifest.json")
        )
        limited_metrics = _mapping(
            _source_bundle_content(limited_bundle, "window_comparison_metrics.json")
        )
        limited_rows = _records(limited_metrics.get("by_window"))
        limited_first = limited_rows[0] if limited_rows else {}
        consensus_manifest = _mapping(
            _source_bundle_content(consensus_bundle, "consensus_risk_manifest.json")
        )
        rows.append(
            {
                "refresh_id": _text(selected.get("refresh_id")),
                "outcome_update_id": _text(selected.get("outcome_update_id")),
                "generated_at": _text(selected.get("generated_at")),
                "as_of": _text(selected.get("as_of")),
                "forward_available": forward.get("available"),
                "forward_pending": forward.get("pending"),
                "historical_replay_available": historical.get("available"),
                "historical_replay_pending": historical.get("pending"),
                "limited_vs_notrade_available_count": limited_manifest.get(
                    "available_count"
                ),
                "limited_vs_notrade_win_rate": limited_first.get("win_rate"),
                "limited_vs_notrade_avg_relative_return": limited_first.get(
                    "avg_relative_return"
                ),
                "limited_vs_notrade_confidence": limited_first.get("confidence"),
                "consensus_target_risk": consensus_manifest.get("consensus_target_risk"),
                "recommended_action": limited_metrics.get("overall_recommendation"),
                "sample_scope": "post_dashboard_full_forward_state",
                "production_effect": "none",
                "broker_action_taken": False,
            }
        )
    return rows


def _confidence_trend_summary(
    rows: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]
) -> dict[str, Any]:
    minimum_history = _int(policy.get("minimum_history_refresh_count"))
    growth_policy = _mapping(policy.get("availability_growth"))
    if len(rows) < minimum_history:
        trend_status = "INSUFFICIENT_HISTORY"
        growth = None
        confidence_change = "NO_CHANGE"
    else:
        first = rows[0]
        last = rows[-1]
        growth = _int(last.get("forward_available")) - _int(first.get("forward_available"))
        confidence_change = _confidence_change(
            _text(first.get("limited_vs_notrade_confidence"), "INSUFFICIENT_DATA"),
            _text(last.get("limited_vs_notrade_confidence"), "INSUFFICIENT_DATA"),
            _texts(policy.get("confidence_order")),
        )
        candidates = {"STABLE"}
        if growth >= _int(growth_policy.get("improving_minimum")) or (
            confidence_change == "IMPROVED"
        ):
            candidates.add("IMPROVING")
        if growth <= _int(growth_policy.get("deteriorating_maximum")) or (
            confidence_change == "DETERIORATED"
        ):
            candidates.add("DETERIORATING")
        trend_status = next(
            status
            for status in _texts(policy.get("trend_precedence"))
            if status in candidates
        )
    latest = rows[-1] if rows else {}
    confidence = latest.get("limited_vs_notrade_confidence")
    avg_return = latest.get("limited_vs_notrade_avg_relative_return")
    signal_policy = _mapping(policy.get("relative_return_signal"))
    limited_signal = "INSUFFICIENT_DATA"
    if confidence in set(_texts(signal_policy.get("eligible_confidence"))) and isinstance(
        avg_return, (int, float)
    ):
        limited_signal = (
            "EARLY_POSITIVE"
            if float(avg_return)
            > float(signal_policy.get("early_positive_exclusive_floor", 0.0))
            else "MIXED"
        )
    consensus_risk = latest.get("consensus_target_risk")
    consensus_signal = (
        consensus_risk
        if consensus_risk in set(_texts(policy.get("consensus_risk_signal_statuses")))
        else "INSUFFICIENT_DATA"
    )
    next_action = _text(
        _mapping(policy.get("next_action_by_trend")).get(trend_status),
        "continue_tracking",
    )
    if consensus_risk in set(_texts(policy.get("manual_review_risk_statuses"))):
        next_action = "manual_review"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confidence_trend_summary",
        "trend_status": trend_status,
        "available_sample_growth": growth,
        "confidence_change": confidence_change,
        "limited_vs_notrade_signal": limited_signal,
        "consensus_risk_signal": consensus_signal,
        "next_action": next_action,
        "policy_id": policy.get("policy_id"),
        "policy_version": policy.get("version"),
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _confidence_change(before: str, after: str, confidence_order: Sequence[str]) -> str:
    order = {value: index for index, value in enumerate(confidence_order)}
    before_rank = order.get(before, 0)
    after_rank = order.get(after, 0)
    if after_rank > before_rank:
        return "IMPROVED"
    if after_rank < before_rank:
        return "DETERIORATED"
    return "NO_CHANGE"


def _load_forward_outcome_decision_policy(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping) or payload.get("schema_version") != (
        "etf_dynamic_v3_forward_outcome_decision_policy.v1"
    ):
        raise DynamicV3OutcomeAccumulationError("forward decision policy schema invalid")
    policy = dict(payload)
    required_text = (
        "policy_id",
        "version",
        "status",
        "owner",
        "rationale",
        "review_condition",
        "validation_plan",
    )
    recommendations = {
        "manual_review",
        "wait_for_more_outcomes",
        "continue_tracking",
        "review_rule_calibration_evidence",
    }
    action_map = _mapping(policy.get("next_actions_by_recommendation"))
    allowed_actions = {
        "manual_review",
        "continue_tracking",
        "do_not_change_policy",
        "review_rule_calibration_evidence",
        "do_not_auto_apply_policy",
    }
    confidence = _texts(policy.get("eligible_confidence"))
    ready_consensus = _texts(policy.get("ready_consensus_statuses"))
    warning_consensus = _texts(policy.get("ready_with_warnings_consensus_statuses"))
    manual_consensus = _texts(policy.get("manual_review_consensus_statuses"))
    eligible_trends = _texts(policy.get("eligible_trend_statuses"))
    manual_trends = _texts(policy.get("manual_review_trend_statuses"))
    if (
        not all(_text(policy.get(key)) for key in required_text)
        or _int(policy.get("week_window_days")) != 7
        or not 0 <= _int(policy.get("maximum_post_week_generation_days")) <= 7
        or _int(policy.get("minimum_forward_available_for_rule_review")) < 2
        or not confidence
        or not set(confidence).issubset({"LOW", "MEDIUM", "HIGH"})
        or len(confidence) != len(set(confidence))
        or not ready_consensus
        or not set(ready_consensus).issubset({"PASS"})
        or not warning_consensus
        or not set(warning_consensus).issubset({"PASS_WITH_WARNINGS"})
        or not manual_consensus
        or not set(manual_consensus).issubset({"REVIEW_REQUIRED", "HIGH_RISK"})
        or not eligible_trends
        or not set(eligible_trends).issubset({"STABLE", "IMPROVING"})
        or not manual_trends
        or not set(manual_trends).issubset({"DETERIORATING"})
        or set(_texts(policy.get("recommendation_precedence"))) != recommendations
        or len(_texts(policy.get("recommendation_precedence"))) != len(recommendations)
        or set(action_map) != recommendations
        or any(
            not _texts(actions)
            or len(_texts(actions)) != len(set(_texts(actions)))
            or not set(_texts(actions)).issubset(allowed_actions)
            for actions in action_map.values()
        )
        or not 1 <= _int(policy.get("next_due_scan_offset_days")) <= 31
        or policy.get("production_effect") != "none"
        or policy.get("broker_action_allowed") is not False
        or policy.get("auto_policy_apply") is not False
    ):
        raise DynamicV3OutcomeAccumulationError("forward decision policy contract invalid")
    return policy


def _forward_outcome_decision_manifest(
    *,
    decision_dir: Path,
    week_ending: date,
    generated: datetime,
    matrix: Mapping[str, Any],
    policy: Mapping[str, Any],
    source_snapshot_checksum: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_forward_decision_manifest",
        "decision_id": decision_dir.name,
        "week_ending": week_ending.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "evidence_status": matrix.get("evidence_status"),
        "recommended_action": matrix.get("recommended_action"),
        "rule_calibration_readiness": matrix.get("rule_calibration_readiness"),
        "policy_id": policy.get("policy_id"),
        "policy_version": policy.get("version"),
        "source_snapshot_path": str(decision_dir / "forward_decision_source_snapshot.json"),
        "source_snapshot_checksum": source_snapshot_checksum,
        "forward_decision_manifest_path": str(decision_dir / "forward_decision_manifest.json"),
        "forward_go_no_go_matrix_path": str(decision_dir / "forward_go_no_go_matrix.json"),
        "forward_next_actions_path": str(decision_dir / "forward_next_actions.json"),
        "forward_outcome_decision_report_path": str(
            decision_dir / "forward_outcome_decision_report.md"
        ),
        "reader_brief_section_path": str(decision_dir / "reader_brief_section.md"),
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "auto_policy_apply": False,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _build_forward_outcome_decision_snapshot(
    *,
    week_ending: date,
    generated: datetime,
    outcome_update_dir: Path,
    rolling_refresh_dir: Path,
    evidence_trend_dir: Path,
    outcome_update_id: str | None,
    refresh_id: str | None,
    trend_id: str | None,
    policy_path: Path,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    if generated.tzinfo is None or generated.utcoffset() is None:
        raise DynamicV3OutcomeAccumulationError(
            "forward decision generated_at must be timezone-aware"
        )
    generated = generated.astimezone(UTC)
    week_start = week_ending - timedelta(days=_int(policy.get("week_window_days")) - 1)
    latest_generation_date = week_ending + timedelta(
        days=_int(policy.get("maximum_post_week_generation_days"))
    )
    if generated.date() < week_start or generated.date() > latest_generation_date:
        raise DynamicV3OutcomeAccumulationError(
            "forward decision generated_at outside reviewed week window"
        )
    week_cutoff = datetime(
        week_ending.year,
        week_ending.month,
        week_ending.day,
        23,
        59,
        59,
        999999,
        tzinfo=UTC,
    )
    cutoff = min(generated, week_cutoff)
    update = _select_forward_decision_source(
        kind="outcome_update",
        root=outcome_update_dir,
        explicit_id=outcome_update_id,
        manifest_name="outcome_update_manifest.json",
        id_key="outcome_update_id",
        cutoff=cutoff,
        week_ending=week_ending,
        validator=lambda artifact_id, root: validate_outcome_update_artifact(
            update_id=artifact_id, output_dir=root
        ),
    )
    refresh = _select_forward_decision_source(
        kind="rolling_refresh",
        root=rolling_refresh_dir,
        explicit_id=refresh_id,
        manifest_name="rolling_refresh_manifest.json",
        id_key="refresh_id",
        cutoff=cutoff,
        week_ending=week_ending,
        validator=lambda artifact_id, root: validate_rolling_evidence_refresh_artifact(
            refresh_id=artifact_id, output_dir=root
        ),
    )
    trend = _select_forward_decision_source(
        kind="evidence_trend",
        root=evidence_trend_dir,
        explicit_id=trend_id,
        manifest_name="evidence_trend_manifest.json",
        id_key="trend_id",
        cutoff=cutoff,
        week_ending=week_ending,
        validator=lambda artifact_id, root: validate_evidence_trend_artifact(
            trend_id=artifact_id, output_dir=root
        ),
    )
    update_selected = _mapping(update.get("selected"))
    refresh_selected = _mapping(refresh.get("selected"))
    trend_selected = _mapping(trend.get("selected"))
    if refresh_selected and not update_selected:
        raise DynamicV3OutcomeAccumulationError("rolling refresh requires selected outcome update")
    if trend_selected and not refresh_selected:
        raise DynamicV3OutcomeAccumulationError("evidence trend requires selected rolling refresh")
    if refresh_selected:
        refresh_manifest = _mapping(refresh_selected.get("manifest"))
        if refresh_manifest.get("outcome_update_id") != update_selected.get("artifact_id"):
            raise DynamicV3OutcomeAccumulationError("outcome update to refresh lineage mismatch")
    if trend_selected:
        trend_bundle = _mapping(trend_selected.get("source_bundle"))
        trend_snapshot = _mapping(
            _source_bundle_content(trend_bundle, "evidence_trend_source_snapshot.json")
        )
        trend_refresh_ids = {
            _text(row.get("refresh_id"))
            for row in _records(trend_snapshot.get("selected_refreshes"))
        }
        if refresh_selected.get("artifact_id") not in trend_refresh_ids:
            raise DynamicV3OutcomeAccumulationError("rolling refresh to trend lineage mismatch")
    return {
        "schema_version": FORWARD_OUTCOME_DECISION_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_forward_outcome_decision_source_snapshot",
        "generated_at": generated.isoformat(),
        "week_start": week_start.isoformat(),
        "week_ending": week_ending.isoformat(),
        "evidence_cutoff": cutoff.isoformat(),
        "sources": {
            "outcome_update": update,
            "rolling_refresh": refresh,
            "evidence_trend": trend,
        },
        "policy_path": str(policy_path),
        "policy_checksum": _file_sha256(policy_path),
        "policy": dict(policy),
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _select_forward_decision_source(
    *,
    kind: str,
    root: Path,
    explicit_id: str | None,
    manifest_name: str,
    id_key: str,
    cutoff: datetime,
    week_ending: date,
    validator: Any,
) -> dict[str, Any]:
    selected_candidates: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    children = [root / explicit_id] if explicit_id else _artifact_children(root)
    if explicit_id and not children[0].is_dir():
        raise DynamicV3OutcomeAccumulationError(f"explicit {kind} not found: {explicit_id}")
    for child in children:
        transaction = _read_optional_json(child / "outcome_update_transaction.json") or (
            _read_optional_json(child / "rolling_refresh_transaction.json") or {}
        )
        if transaction.get("status") == "ROLLED_BACK":
            if explicit_id:
                raise DynamicV3OutcomeAccumulationError(f"explicit {kind} is rolled back")
            excluded.append({"artifact_id": child.name, "reason": "ROLLED_BACK"})
            continue
        if transaction.get("status") == "PREPARED":
            raise DynamicV3OutcomeAccumulationError(f"prepared {kind} blocks decision")
        manifest = _read_optional_json(child / manifest_name)
        if not manifest:
            raise DynamicV3OutcomeAccumulationError(f"{kind} manifest invalid: {child.name}")
        artifact_id = _text(manifest.get(id_key))
        source_generated = _datetime_from_any(manifest.get("generated_at"))
        if artifact_id != child.name or source_generated is None:
            raise DynamicV3OutcomeAccumulationError(f"{kind} identity/time invalid: {child.name}")
        if source_generated > cutoff:
            if explicit_id:
                raise DynamicV3OutcomeAccumulationError(f"explicit {kind} exceeds decision cutoff")
            excluded.append({"artifact_id": artifact_id, "reason": "FUTURE_GENERATED"})
            continue
        source_as_of = _date_from_any(manifest.get("as_of"))
        if source_as_of is not None and source_as_of > week_ending:
            raise DynamicV3OutcomeAccumulationError(f"{kind} as_of exceeds week ending")
        validation = validator(artifact_id, root)
        if validation.get("status") == "PASS_WITH_WARNINGS" and not explicit_id:
            excluded.append({"artifact_id": artifact_id, "reason": "LEGACY_UNSNAPSHOTTED"})
            continue
        if validation.get("status") != "PASS":
            raise DynamicV3OutcomeAccumulationError(
                f"{kind} validation must PASS: {artifact_id}={validation.get('status')}"
            )
        selected_candidates.append(
            {
                "artifact_id": artifact_id,
                "generated_at": source_generated.isoformat(),
                "manifest": manifest,
                "source_bundle": _immutable_source_bundle(child),
                "validation": validation,
            }
        )
    if not selected_candidates:
        return {
            "status": "MISSING",
            "root": str(root),
            "explicit_id": explicit_id,
            "selected": {},
            "excluded": excluded,
        }
    latest_time = max(_text(row.get("generated_at")) for row in selected_candidates)
    latest = [row for row in selected_candidates if row.get("generated_at") == latest_time]
    if len(latest) != 1:
        raise DynamicV3OutcomeAccumulationError(f"ambiguous semantic latest {kind}")
    return {
        "status": "SELECTED",
        "root": str(root),
        "explicit_id": explicit_id,
        "selected": latest[0],
        "excluded": excluded,
    }


def _forward_outcome_decision_snapshot_errors(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    generated = _datetime_from_any(snapshot.get("generated_at"))
    cutoff = _datetime_from_any(snapshot.get("evidence_cutoff"))
    week_start = _date_from_any(snapshot.get("week_start"))
    week_ending = _date_from_any(snapshot.get("week_ending"))
    if (
        snapshot.get("schema_version") != FORWARD_OUTCOME_DECISION_SNAPSHOT_SCHEMA_VERSION
        or generated is None
        or cutoff is None
        or cutoff > generated
        or week_start is None
        or week_ending is None
        or week_start > week_ending
        or cutoff.date() > week_ending
        or snapshot.get("production_effect") != "none"
        or snapshot.get("broker_action_taken") is not False
    ):
        errors.append("snapshot_contract_invalid")
    policy_path = Path(_text(snapshot.get("policy_path")))
    try:
        policy_matches = (
            _file_sha256(policy_path) == snapshot.get("policy_checksum")
            and _load_forward_outcome_decision_policy(policy_path) == snapshot.get("policy")
        )
    except Exception:  # noqa: BLE001
        policy_matches = False
    if not policy_matches:
        errors.append("policy_snapshot_changed")
    source_specs = {
        "outcome_update": (
            "outcome_update_manifest.json",
            "outcome_update_id",
            lambda artifact_id, root: validate_outcome_update_artifact(
                update_id=artifact_id, output_dir=root
            ),
        ),
        "rolling_refresh": (
            "rolling_refresh_manifest.json",
            "refresh_id",
            lambda artifact_id, root: validate_rolling_evidence_refresh_artifact(
                refresh_id=artifact_id, output_dir=root
            ),
        ),
        "evidence_trend": (
            "evidence_trend_manifest.json",
            "trend_id",
            lambda artifact_id, root: validate_evidence_trend_artifact(
                trend_id=artifact_id, output_dir=root
            ),
        ),
    }
    selected: dict[str, dict[str, Any]] = {}
    for kind, (manifest_name, id_key, validator) in source_specs.items():
        row = _mapping(_mapping(snapshot.get("sources")).get(kind))
        source_root = _text(row.get("root"))
        if not source_root:
            errors.append(f"{kind}_root_missing")
        elif cutoff is not None and week_ending is not None:
            try:
                live_selection = _select_forward_decision_source(
                    kind=kind,
                    root=Path(source_root),
                    explicit_id=_text(row.get("explicit_id")) or None,
                    manifest_name=manifest_name,
                    id_key=id_key,
                    cutoff=cutoff,
                    week_ending=week_ending,
                    validator=validator,
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{kind}_selection_failed:{exc}")
                live_selection = {}
            if any(
                live_selection.get(field) != row.get(field)
                for field in ("status", "root", "explicit_id", "selected")
            ):
                errors.append(f"{kind}_selection_changed")
        selected_row = _mapping(row.get("selected"))
        if row.get("status") == "MISSING" and not selected_row:
            selected[kind] = {}
            continue
        bundle = _mapping(selected_row.get("source_bundle"))
        root = Path(_text(bundle.get("root")))
        artifact_id = _text(selected_row.get("artifact_id"))
        manifest = _mapping(_source_bundle_content(bundle, manifest_name))
        source_generated = _datetime_from_any(selected_row.get("generated_at"))
        source_as_of = _date_from_any(manifest.get("as_of"))
        live_validation = validator(artifact_id, root.parent)
        if (
            row.get("status") != "SELECTED"
            or not artifact_id
            or manifest.get(id_key) != artifact_id
            or root.name != artifact_id
            or source_generated is None
            or cutoff is None
            or source_generated > cutoff
            or (source_as_of is not None and week_ending is not None and source_as_of > week_ending)
            or not _source_bundle_matches(bundle)
            or live_validation.get("status") != "PASS"
            or _mapping(selected_row.get("validation")) != live_validation
        ):
            errors.append(f"{kind}_source_changed")
        selected[kind] = selected_row
    update = selected.get("outcome_update") or {}
    refresh = selected.get("rolling_refresh") or {}
    trend = selected.get("evidence_trend") or {}
    if refresh and (
        not update
        or _mapping(refresh.get("manifest")).get("outcome_update_id")
        != update.get("artifact_id")
    ):
        errors.append("update_refresh_lineage_invalid")
    if trend:
        trend_snapshot = _mapping(
            _source_bundle_content(
                _mapping(trend.get("source_bundle")),
                "evidence_trend_source_snapshot.json",
            )
        )
        refresh_ids = {
            _text(row.get("refresh_id"))
            for row in _records(trend_snapshot.get("selected_refreshes"))
        }
        if not refresh or refresh.get("artifact_id") not in refresh_ids:
            errors.append("refresh_trend_lineage_invalid")
    return errors


def _forward_go_no_go_matrix(
    *,
    week_ending: date,
    snapshot: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    sources = _mapping(snapshot.get("sources"))
    update_source = _mapping(_mapping(sources.get("outcome_update")).get("selected"))
    refresh_source = _mapping(_mapping(sources.get("rolling_refresh")).get("selected"))
    trend_source = _mapping(_mapping(sources.get("evidence_trend")).get("selected"))
    updated_count: int | None = None
    skipped_count: int | None = None
    update_status = "MISSING"
    if update_source:
        update_bundle = _mapping(update_source.get("source_bundle"))
        updated_count = len(
            _records(_source_bundle_content(update_bundle, "updated_windows.jsonl"))
        )
        skipped_count = len(
            _records(_source_bundle_content(update_bundle, "skipped_windows.jsonl"))
        )
        update_status = "UPDATED" if updated_count else "NO_DUE_WINDOWS"
    if updated_count and skipped_count:
        update_status = "PARTIAL"
    trend_summary: dict[str, Any] = {}
    latest: dict[str, Any] = {}
    if trend_source:
        trend_bundle = _mapping(trend_source.get("source_bundle"))
        trend_summary = _mapping(
            _source_bundle_content(trend_bundle, "confidence_trend_summary.json")
        )
        rows = _records(
            _source_bundle_content(trend_bundle, "evidence_trend_timeseries.jsonl")
        )
        latest = rows[-1] if rows else {}
    forward_available = latest.get("forward_available")
    forward_pending = latest.get("forward_pending")
    confidence = latest.get("limited_vs_notrade_confidence")
    consensus = latest.get("consensus_target_risk")
    trend_status = trend_summary.get("trend_status")
    all_sources_selected = bool(update_source and refresh_source and trend_source)
    readiness = "NOT_READY"
    enough_samples = isinstance(forward_available, int) and forward_available >= _int(
        policy.get("minimum_forward_available_for_rule_review")
    )
    confidence_ready = confidence in set(_texts(policy.get("eligible_confidence")))
    trend_ready = trend_status in set(_texts(policy.get("eligible_trend_statuses")))
    consensus_ready = consensus in set(_texts(policy.get("ready_consensus_statuses")))
    consensus_warning = consensus in set(
        _texts(policy.get("ready_with_warnings_consensus_statuses"))
    )
    if all_sources_selected and enough_samples and confidence_ready and trend_ready:
        if consensus_ready:
            readiness = "READY"
        elif consensus_warning:
            readiness = "READY_WITH_WARNINGS"
    candidates = {"continue_tracking"}
    if (
        consensus in set(_texts(policy.get("manual_review_consensus_statuses")))
        or trend_status in set(_texts(policy.get("manual_review_trend_statuses")))
        or readiness == "READY_WITH_WARNINGS"
    ):
        candidates.add("manual_review")
    if not all_sources_selected or forward_available in {None, 0} or update_status == (
        "NO_DUE_WINDOWS"
    ):
        candidates.add("wait_for_more_outcomes")
    if readiness == "READY":
        candidates.add("review_rule_calibration_evidence")
    recommended = next(
        value
        for value in _texts(policy.get("recommendation_precedence"))
        if value in candidates
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_forward_go_no_go_matrix",
        "week_ending": week_ending.isoformat(),
        "evidence_status": "COMPLETE" if all_sources_selected else "INCOMPLETE",
        "outcome_update_id": update_source.get("artifact_id") or "MISSING",
        "rolling_refresh_id": refresh_source.get("artifact_id") or "MISSING",
        "evidence_trend_id": trend_source.get("artifact_id") or "MISSING",
        "outcome_update_status": update_status,
        "updated_count": updated_count,
        "skipped_count": skipped_count,
        "forward_available": forward_available,
        "forward_pending": forward_pending,
        "limited_vs_notrade_confidence": confidence,
        "consensus_target_risk": consensus,
        "evidence_trend_status": trend_status,
        "minimum_forward_available_for_rule_review": policy.get(
            "minimum_forward_available_for_rule_review"
        ),
        "rule_calibration_readiness": readiness,
        "broker_action_allowed": False,
        "production_effect": "none",
        "recommended_action": recommended,
    }


def _forward_next_actions(
    matrix: Mapping[str, Any], policy: Mapping[str, Any]
) -> dict[str, Any]:
    week_ending = _date_from_any(matrix.get("week_ending"))
    if week_ending is None:
        raise DynamicV3OutcomeAccumulationError("forward decision week ending invalid")
    next_due_scan = week_ending + timedelta(
        days=_int(policy.get("next_due_scan_offset_days"))
    )
    recommended = _text(matrix.get("recommended_action"))
    configured_actions = _texts(
        _mapping(policy.get("next_actions_by_recommendation")).get(recommended)
    )
    actions = [
        {
            "action": action,
            "priority": "HIGH",
            "reason": f"reviewed decision policy route: {recommended}",
        }
        for action in configured_actions
    ]
    actions.append(
        {
            "action": "run_next_due_scan",
            "priority": "HIGH",
            "target_date": next_due_scan.isoformat(),
        }
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_forward_next_actions",
        "next_actions": actions,
        "policy_id": policy.get("policy_id"),
        "policy_version": policy.get("version"),
        "production_effect": "none",
        "broker_action_taken": False,
    }


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
    elif start is None or window_days <= 0 or latest_price_date is None:
        due_status = "INSUFFICIENT_DATA"
        reason = "missing_advisory_id_as_of_window_or_price_calendar"
    elif expected_end is None or expected_end > latest_price_date:
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


def _load_replay_sample_expansion_policy(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise DynamicV3OutcomeAccumulationError(f"replay sample expansion policy not found: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    policy = dict(payload) if isinstance(payload, Mapping) else {}
    required_text = (
        "schema_version",
        "policy_id",
        "version",
        "status",
        "owner",
        "rationale",
        "intended_effect",
        "review_condition",
    )
    if any(not _text(policy.get(key)) for key in required_text):
        raise DynamicV3OutcomeAccumulationError(
            "replay sample expansion policy metadata is incomplete"
        )
    if policy.get("automatic_replay_execution_allowed") is not False:
        raise DynamicV3OutcomeAccumulationError(
            "replay sample expansion policy must prohibit automatic replay execution"
        )
    if policy.get("production_effect") != "none":
        raise DynamicV3OutcomeAccumulationError(
            "replay sample expansion policy production_effect must be none"
        )
    for key in (
        "pit_unsafe_limitations",
        "replay_ineligible_limitations",
        "pit_warning_limitations",
        "source_precedence",
    ):
        values = _texts(policy.get(key))
        if not values or len(values) != len(set(values)):
            raise DynamicV3OutcomeAccumulationError(
                f"replay sample expansion policy {key} must be unique and non-empty"
            )
        policy[key] = values
    return policy


def _build_replay_sample_expansion_snapshot(
    *,
    start: date,
    end: date,
    generated: datetime,
    daily_advisory_dir: Path,
    owner_review_dir: Path,
    replay_inventory_dir: Path,
    prices_path: Path,
    rates_path: Path,
    policy_path: Path,
    policy: Mapping[str, Any],
    quality_status: str,
) -> dict[str, Any]:
    daily_sources: list[dict[str, Any]] = []
    daily_ids: set[str] = set()
    daily_dates: set[str] = set()
    daily_manifest_paths = sorted(daily_advisory_dir.glob("*/daily_advisory_manifest.json"))
    for manifest_path in daily_manifest_paths:
        manifest = _read_optional_json(manifest_path)
        if manifest is None:
            raise DynamicV3OutcomeAccumulationError(
                f"daily advisory manifest is invalid JSON: {manifest_path}"
            )
        as_of = _date_from_any(manifest.get("as_of"))
        if as_of is None or not (start <= as_of <= end):
            continue
        daily_id = _text(manifest.get("daily_advisory_id"), manifest_path.parent.name)
        source_time = _datetime_from_any(manifest.get("generated_at"))
        if not daily_id or source_time is None or source_time > generated:
            raise DynamicV3OutcomeAccumulationError(
                f"daily advisory identity/time is invalid or future: {manifest_path}"
            )
        if daily_id in daily_ids or as_of.isoformat() in daily_dates:
            raise DynamicV3OutcomeAccumulationError(
                "duplicate daily advisory id or as_of in replay expansion range"
            )
        validation = validate_position_advisory_daily_artifact(
            daily_advisory_id=daily_id,
            output_dir=daily_advisory_dir,
        )
        if validation.get("status") != "PASS":
            raise DynamicV3OutcomeAccumulationError(
                f"daily advisory validation must PASS: {daily_id}"
            )
        daily_ids.add(daily_id)
        daily_dates.add(as_of.isoformat())
        daily_sources.append(_immutable_source_bundle(manifest_path.parent))

    owner_records: list[dict[str, Any]] = []
    owner_sources: list[dict[str, Any]] = []
    owner_events_path = owner_review_dir / "owner_review_events.jsonl"
    if owner_events_path.is_file():
        selected_by_daily: dict[str, dict[str, Any]] = {}
        for record in _read_jsonl(owner_review_dir / "owner_review_journal.jsonl"):
            daily_id = _text(record.get("daily_advisory_id"))
            if daily_id not in daily_ids:
                continue
            source_time = _datetime_from_any(record.get("updated_at") or record.get("created_at"))
            if source_time is None or source_time > generated:
                continue
            review_id = _text(record.get("review_id"))
            validation = validate_owner_review_artifact(
                review_id=review_id,
                output_dir=owner_review_dir,
            )
            if validation.get("status") != "PASS":
                raise DynamicV3OutcomeAccumulationError(
                    f"owner review validation must PASS: {review_id}"
                )
            if daily_id in selected_by_daily:
                raise DynamicV3OutcomeAccumulationError(
                    f"multiple validated owner reviews for daily advisory: {daily_id}"
                )
            selected_by_daily[daily_id] = dict(record)
        owner_records = [selected_by_daily[key] for key in sorted(selected_by_daily)]
        owner_sources.append(_immutable_source_bundle(owner_review_dir))

    replay_candidates: list[tuple[datetime, str, dict[str, Any]]] = []
    replay_manifest_paths = sorted(replay_inventory_dir.glob("*/replay_inventory_manifest.json"))
    for manifest_path in replay_manifest_paths:
        manifest = _read_optional_json(manifest_path)
        if manifest is None:
            raise DynamicV3OutcomeAccumulationError(
                f"replay inventory manifest is invalid JSON: {manifest_path}"
            )
        source_time = _datetime_from_any(manifest.get("generated_at"))
        inventory_id = _text(manifest.get("inventory_id"), manifest_path.parent.name)
        if source_time is None or source_time > generated:
            continue
        validation = validate_replay_inventory_artifact(
            inventory_id=inventory_id,
            output_dir=replay_inventory_dir,
        )
        if validation.get("status") != "PASS":
            raise DynamicV3OutcomeAccumulationError(
                f"replay inventory validation must PASS: {inventory_id}"
            )
        replay_candidates.append(
            (source_time, inventory_id, _immutable_source_bundle(manifest_path.parent))
        )
    replay_sources: list[dict[str, Any]] = []
    if replay_candidates:
        replay_candidates.sort(key=lambda row: (row[0], row[1]))
        latest_time = replay_candidates[-1][0]
        latest = [row for row in replay_candidates if row[0] == latest_time]
        if len(latest) != 1:
            raise DynamicV3OutcomeAccumulationError(
                "ambiguous latest replay inventory at generated cutoff"
            )
        replay_sources = [latest[0][2]]

    price_index = {
        symbol: sorted(item.isoformat() for item in dates if item <= generated.date())
        for symbol, dates in sorted(_price_availability_index(prices_path).items())
    }
    return {
        "schema_version": REPLAY_SAMPLE_EXPANSION_SNAPSHOT_SCHEMA_VERSION,
        "generated_at": generated.isoformat(),
        "generated_cutoff": generated.isoformat(),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "daily_advisory_dir": str(daily_advisory_dir),
        "daily_manifest_scanned_count": len(daily_manifest_paths),
        "daily_sources": daily_sources,
        "owner_review_dir": str(owner_review_dir),
        "owner_records": owner_records,
        "owner_review_sources": owner_sources,
        "replay_inventory_dir": str(replay_inventory_dir),
        "replay_inventory_manifest_scanned_count": len(replay_manifest_paths),
        "replay_inventory_sources": replay_sources,
        "prices_path": str(prices_path),
        "prices_checksum": _file_sha256(prices_path),
        "rates_path": str(rates_path),
        "rates_checksum": _file_sha256(rates_path),
        "price_date_availability": price_index,
        "policy_path": str(policy_path),
        "policy_checksum": _file_sha256(policy_path),
        "policy": dict(policy),
        "data_quality_status": quality_status,
        "production_effect": "none",
    }


def _replay_sample_expansion_views_from_snapshot(
    snapshot: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    start = _date_from_any(snapshot.get("start"))
    end = _date_from_any(snapshot.get("end"))
    generated = _datetime_from_any(snapshot.get("generated_cutoff"))
    if start is None or end is None or generated is None or start > end or end > generated.date():
        raise DynamicV3OutcomeAccumulationError("invalid replay expansion snapshot range/cutoff")
    policy = _mapping(snapshot.get("policy"))
    price_index = {
        _text(symbol): {
            parsed for value in _texts(values) if (parsed := _date_from_any(value)) is not None
        }
        for symbol, values in _mapping(snapshot.get("price_date_availability")).items()
    }
    owner_by_daily = {
        _text(row.get("daily_advisory_id")): dict(row)
        for row in _records(snapshot.get("owner_records"))
    }
    rows: list[dict[str, Any]] = []
    for bundle in _records(snapshot.get("daily_sources")):
        manifest = _mapping(_source_bundle_content(bundle, "daily_advisory_manifest.json"))
        as_of = _date_from_any(manifest.get("as_of"))
        if as_of is None or not (start <= as_of <= end):
            raise DynamicV3OutcomeAccumulationError("daily source escaped frozen range")
        daily_id = _text(manifest.get("daily_advisory_id"))
        source_time = _datetime_from_any(manifest.get("generated_at"))
        if not daily_id or source_time is None or source_time > generated:
            raise DynamicV3OutcomeAccumulationError("daily source escaped generated cutoff")
        targets, current, consensus, candidate_id = _daily_inputs_from_bundle(bundle)
        owner = owner_by_daily.get(daily_id, {})
        limitations: list[str] = []
        if not targets:
            limitations.append("MISSING_TARGET_WEIGHTS")
        if not current:
            limitations.append("MISSING_CURRENT_WEIGHTS")
        if not consensus:
            limitations.append("CONSENSUS_WEIGHTS_MISSING")
        if not owner:
            limitations.append("OWNER_DECISION_MISSING")
        if source_time.date() > as_of:
            limitations.append("SOURCE_GENERATED_AFTER_AS_OF_DATE")
        symbols = set(targets) | set(current) | set(consensus)
        price_after = _has_price_after(symbols, as_of, price_index)
        if not price_after:
            limitations.append("MISSING_PRICE_DATA_AFTER_AS_OF")
        pit_status, eligibility = _replay_sample_classification(limitations, policy)
        rows.append(
            _expanded_event_row(
                source_type="daily_advisory",
                source_path=_bundle_primary_path(bundle, "daily_advisory_manifest.json"),
                daily_advisory_id=daily_id,
                as_of=as_of,
                candidate_id=candidate_id,
                targets=targets,
                current=current,
                consensus=consensus,
                owner_available=bool(owner),
                price_after=price_after,
                pit_status=pit_status,
                eligibility=eligibility,
                limitations=limitations,
            )
        )
    for bundle in _records(snapshot.get("replay_inventory_sources")):
        source_path = _bundle_primary_path(bundle, "replay_artifact_inventory.jsonl")
        for source in _records(_source_bundle_content(bundle, "replay_artifact_inventory.jsonl")):
            as_of = _date_from_any(source.get("as_of"))
            if as_of is None or not (start <= as_of <= end):
                continue
            inputs = _mapping(source.get("decision_inputs"))
            targets = _normalize_weights(_mapping(inputs.get("target_weights")))
            current = _normalize_weights(_mapping(inputs.get("current_weights")))
            consensus = _normalize_weights(_mapping(inputs.get("consensus_weights")))
            owner_available = _text(inputs.get("owner_decision")) not in {"", "missing"}
            symbols = set(targets) | set(current) | set(consensus)
            price_after = _has_price_after(symbols, as_of, price_index)
            limitations = _texts(source.get("replay_limitations"))
            if not targets and "MISSING_TARGET_WEIGHTS" not in limitations:
                limitations.append("MISSING_TARGET_WEIGHTS")
            if not current and "MISSING_CURRENT_WEIGHTS" not in limitations:
                limitations.append("MISSING_CURRENT_WEIGHTS")
            if not consensus and "CONSENSUS_WEIGHTS_MISSING" not in limitations:
                limitations.append("CONSENSUS_WEIGHTS_MISSING")
            if not owner_available and "OWNER_DECISION_MISSING" not in limitations:
                limitations.append("OWNER_DECISION_MISSING")
            if not price_after and "MISSING_PRICE_DATA_AFTER_AS_OF" not in limitations:
                limitations.append("MISSING_PRICE_DATA_AFTER_AS_OF")
            pit_status, eligibility = _replay_sample_classification(limitations, policy)
            rows.append(
                _expanded_event_row(
                    source_type="replay_inventory",
                    source_path=source_path,
                    daily_advisory_id=_text(source.get("daily_advisory_id")),
                    as_of=as_of,
                    candidate_id=_text(source.get("candidate_id")),
                    targets=targets,
                    current=current,
                    consensus=consensus,
                    owner_available=owner_available,
                    price_after=price_after,
                    pit_status=pit_status,
                    eligibility=eligibility,
                    limitations=limitations,
                )
            )
    rows = _canonical_replay_expansion_events(rows, policy)
    inventory = [
        _source_inventory_row(
            source_type="daily_advisory",
            source_root=Path(_text(snapshot.get("daily_advisory_dir"))),
            scanned_count=_int(snapshot.get("daily_manifest_scanned_count")),
            discovered_count=sum(1 for row in rows if row.get("source_type") == "daily_advisory"),
        ),
        _source_inventory_row(
            source_type="owner_review",
            source_root=Path(_text(snapshot.get("owner_review_dir"))),
            scanned_count=len(_records(snapshot.get("owner_records"))),
            discovered_count=len(_records(snapshot.get("owner_records"))),
        ),
        _source_inventory_row(
            source_type="replay_inventory",
            source_root=Path(_text(snapshot.get("replay_inventory_dir"))),
            scanned_count=_int(snapshot.get("replay_inventory_manifest_scanned_count")),
            discovered_count=sum(1 for row in rows if row.get("source_type") == "replay_inventory"),
        ),
        _source_inventory_row(
            source_type="cached_prices",
            source_root=Path(_text(snapshot.get("prices_path"))),
            scanned_count=1,
            discovered_count=1 if _text(snapshot.get("prices_checksum")) else 0,
        ),
    ]
    return inventory, rows


def _replay_expansion_live_source_validation(snapshot: Mapping[str, Any]) -> bool:
    daily_root = Path(_text(snapshot.get("daily_advisory_dir")))
    for bundle in _records(snapshot.get("daily_sources")):
        manifest = _mapping(_source_bundle_content(bundle, "daily_advisory_manifest.json"))
        daily_id = _text(manifest.get("daily_advisory_id"))
        if (
            validate_position_advisory_daily_artifact(
                daily_advisory_id=daily_id,
                output_dir=daily_root,
            ).get("status")
            != "PASS"
        ):
            return False
    owner_root = Path(_text(snapshot.get("owner_review_dir")))
    for record in _records(snapshot.get("owner_records")):
        if (
            validate_owner_review_artifact(
                review_id=_text(record.get("review_id")),
                output_dir=owner_root,
            ).get("status")
            != "PASS"
        ):
            return False
    replay_root = Path(_text(snapshot.get("replay_inventory_dir")))
    for bundle in _records(snapshot.get("replay_inventory_sources")):
        manifest = _mapping(_source_bundle_content(bundle, "replay_inventory_manifest.json"))
        if (
            validate_replay_inventory_artifact(
                inventory_id=_text(manifest.get("inventory_id")),
                output_dir=replay_root,
            ).get("status")
            != "PASS"
        ):
            return False
    return True


def _daily_inputs_from_bundle(
    bundle: Mapping[str, Any],
) -> tuple[dict[str, float], dict[str, float], dict[str, float], str]:
    target_rows = _records(_source_bundle_content(bundle, "daily_candidate_targets.jsonl"))
    delta_rows = _records(_source_bundle_content(bundle, "daily_position_deltas.jsonl"))
    targets: dict[str, float] = {}
    current: dict[str, float] = {}
    candidate_id = ""
    for row in target_rows + delta_rows:
        if not candidate_id:
            candidate_id = _text(row.get("candidate_id"))
        if not targets and _mapping(row.get("target_weights")):
            targets = _normalize_weights(_mapping(row.get("target_weights")))
        if not current and _mapping(row.get("current_weights")):
            current = _normalize_weights(_mapping(row.get("current_weights")))
    consensus_text = _text(_source_bundle_content(bundle, "daily_consensus_weights.csv"))
    consensus: dict[str, float] = {}
    if consensus_text:
        for row in csv.DictReader(io.StringIO(consensus_text)):
            symbol = _text(row.get("symbol"))
            value = _float(row.get("median_target_weight"), default=float("nan"))
            if symbol and math.isfinite(value):
                consensus[symbol] = value
        consensus = _normalize_weights(consensus)
    if not targets:
        targets = consensus
    return targets, current, consensus, candidate_id


def _bundle_primary_path(bundle: Mapping[str, Any], relative_path: str) -> str:
    return _text(_mapping(_mapping(bundle.get("files")).get(relative_path)).get("path"))


def _replay_sample_classification(
    limitations: Sequence[str], policy: Mapping[str, Any]
) -> tuple[str, str]:
    limitation_set = set(limitations)
    pit_unsafe = set(_texts(policy.get("pit_unsafe_limitations")))
    ineligible = set(_texts(policy.get("replay_ineligible_limitations")))
    warnings = set(_texts(policy.get("pit_warning_limitations")))
    pit_status = "PIT_UNSAFE" if limitation_set & pit_unsafe else "PIT_SAFE"
    if pit_status != "PIT_UNSAFE" and limitation_set & warnings:
        pit_status = "PIT_WARNING"
    if limitation_set & ineligible:
        eligibility = "INELIGIBLE"
    elif pit_status == "PIT_WARNING":
        eligibility = "PARTIAL"
    else:
        eligibility = "ELIGIBLE"
    return pit_status, eligibility


def _expanded_event_row(
    *,
    source_type: str,
    source_path: str,
    daily_advisory_id: str,
    as_of: date,
    candidate_id: str,
    targets: Mapping[str, float],
    current: Mapping[str, float],
    consensus: Mapping[str, float],
    owner_available: bool,
    price_after: bool,
    pit_status: str,
    eligibility: str,
    limitations: Sequence[str],
) -> dict[str, Any]:
    sample_key = f"{daily_advisory_id}|{as_of.isoformat()}"
    return {
        "expanded_event_id": _stable_id("expanded-event", sample_key),
        "sample_key": sample_key,
        "as_of": as_of.isoformat(),
        "daily_advisory_id": daily_advisory_id,
        "source_type": source_type,
        "source_types": [source_type],
        "source_artifact_path": source_path,
        "source_artifact_paths": [source_path],
        "candidate_id": candidate_id,
        "target_weights_available": bool(targets),
        "current_weights_available": bool(current),
        "consensus_weights_available": bool(consensus),
        "owner_decision_available": owner_available,
        "price_data_after_as_of_available": price_after,
        "pit_safety_status": pit_status,
        "replay_eligibility": eligibility,
        "limitations": sorted(set(limitations)),
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _canonical_replay_expansion_events(
    rows: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]
) -> list[dict[str, Any]]:
    by_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = _text(row.get("sample_key"))
        if not key:
            raise DynamicV3OutcomeAccumulationError("expanded replay event missing sample key")
        by_key[key].append(dict(row))
    precedence = {name: index for index, name in enumerate(_texts(policy.get("source_precedence")))}
    result: list[dict[str, Any]] = []
    comparison_fields = (
        "candidate_id",
        "target_weights_available",
        "current_weights_available",
        "consensus_weights_available",
        "owner_decision_available",
        "price_data_after_as_of_available",
        "pit_safety_status",
        "replay_eligibility",
        "limitations",
    )
    for key, candidates in sorted(by_key.items()):
        identities = {
            json.dumps({field: row.get(field) for field in comparison_fields}, sort_keys=True)
            for row in candidates
        }
        if len(identities) > 1:
            raise DynamicV3OutcomeAccumulationError(
                f"conflicting cross-source replay expansion event: {key}"
            )
        candidates.sort(
            key=lambda row: (
                precedence.get(_text(row.get("source_type")), 999),
                _text(row.get("source_artifact_path")),
            )
        )
        selected = candidates[0]
        selected["source_types"] = sorted({_text(row.get("source_type")) for row in candidates})
        selected["source_artifact_paths"] = sorted(
            {_text(row.get("source_artifact_path")) for row in candidates}
        )
        result.append(selected)
    return sorted(result, key=lambda row: (_text(row.get("as_of")), _text(row.get("sample_key"))))


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


def _load_outcome_dashboard_policy(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise DynamicV3OutcomeAccumulationError(f"outcome dashboard policy not found: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    policy = dict(payload) if isinstance(payload, Mapping) else {}
    for key in (
        "schema_version",
        "policy_id",
        "version",
        "status",
        "owner",
        "rationale",
        "intended_effect",
        "review_condition",
    ):
        if not _text(policy.get(key)):
            raise DynamicV3OutcomeAccumulationError(
                "outcome dashboard policy metadata is incomplete"
            )
    actions = _mapping(policy.get("pending_reason_actions"))
    precedence = _texts(policy.get("pending_reason_precedence"))
    if not actions or set(precedence) != set(actions) or len(precedence) != len(set(precedence)):
        raise DynamicV3OutcomeAccumulationError(
            "outcome dashboard pending reason policy is inconsistent"
        )
    if policy.get("automatic_upstream_run_allowed") is not False:
        raise DynamicV3OutcomeAccumulationError(
            "outcome dashboard policy must prohibit automatic upstream runs"
        )
    if policy.get("production_effect") != "none":
        raise DynamicV3OutcomeAccumulationError(
            "outcome dashboard policy production_effect must be none"
        )
    policy["pending_reason_actions"] = dict(actions)
    policy["pending_reason_precedence"] = precedence
    return policy


def _build_outcome_dashboard_snapshot(
    *,
    generated: datetime,
    advisory_outcome_dir: Path,
    backfill_dir: Path,
    repair_dir: Path,
    paper_sim_dir: Path,
    diagnosis_dir: Path,
    outcome_due_dir: Path,
    include_outcome_due_sources: bool,
    policy_path: Path,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    forward_sources: list[dict[str, Any]] = []
    forward_identities: set[str] = set()
    for child in _artifact_children(advisory_outcome_dir):
        manifest = _read_optional_json(child / "advisory_outcome_manifest.json")
        if manifest is None:
            raise DynamicV3OutcomeAccumulationError(
                f"advisory outcome manifest invalid: {child}"
            )
        outcome_id = _text(manifest.get("outcome_id"), child.name)
        source_time = _datetime_from_any(manifest.get("updated_at") or manifest.get("generated_at"))
        if source_time is None or source_time > generated:
            raise DynamicV3OutcomeAccumulationError(
                f"advisory outcome source time invalid or future: {outcome_id}"
            )
        if outcome_id in forward_identities:
            raise DynamicV3OutcomeAccumulationError("duplicate advisory outcome id")
        validation = validate_advisory_outcome_artifact(
            outcome_id=outcome_id, output_dir=advisory_outcome_dir
        )
        if validation.get("status") != "PASS":
            raise DynamicV3OutcomeAccumulationError(
                f"advisory outcome validation must PASS: {outcome_id}"
            )
        forward_identities.add(outcome_id)
        forward_sources.append(_immutable_source_bundle(child))

    repair_sources = _latest_dashboard_source_bundle(
        root=repair_dir,
        manifest_name="backfill_repair_manifest.json",
        id_field="repair_id",
        generated=generated,
        validator=lambda artifact_id: validate_backfill_repair_artifact(
            repair_id=artifact_id, output_dir=repair_dir
        ),
    )
    backfill_sources: list[dict[str, Any]] = []
    if not repair_sources:
        backfill_sources = _latest_dashboard_source_bundle(
            root=backfill_dir,
            manifest_name="backfill_manifest.json",
            id_field="backfill_id",
            generated=generated,
            validator=lambda artifact_id: validate_backfill_outcome_artifact(
                backfill_id=artifact_id, output_dir=backfill_dir
            ),
        )
    simulation_sources = _latest_dashboard_source_bundle(
        root=paper_sim_dir,
        manifest_name="historical_paper_sim_manifest.json",
        id_field="sim_id",
        generated=generated,
        validator=lambda artifact_id: validate_historical_paper_sim_artifact(
            sim_id=artifact_id, output_dir=paper_sim_dir
        ),
    )
    diagnosis_sources = _latest_dashboard_source_bundle(
        root=diagnosis_dir,
        manifest_name="replay_diagnosis_manifest.json",
        id_field="diagnosis_id",
        generated=generated,
        validator=lambda artifact_id: validate_replay_diagnosis_artifact(
            diagnosis_id=artifact_id, output_dir=diagnosis_dir
        ),
    )
    due_sources = (
        _latest_dashboard_source_bundle(
            root=outcome_due_dir,
            manifest_name="outcome_due_manifest.json",
            id_field="due_id",
            generated=generated,
            validator=lambda artifact_id: validate_outcome_due_artifact(
                due_id=artifact_id, output_dir=outcome_due_dir
            ),
        )
        if include_outcome_due_sources
        else []
    )
    return {
        "schema_version": OUTCOME_DASHBOARD_SNAPSHOT_SCHEMA_VERSION,
        "generated_at": generated.isoformat(),
        "generated_cutoff": generated.isoformat(),
        "advisory_outcome_dir": str(advisory_outcome_dir),
        "forward_sources": forward_sources,
        "backfill_dir": str(backfill_dir),
        "repair_dir": str(repair_dir),
        "historical_sources": repair_sources or backfill_sources,
        "historical_source_type": "repair" if repair_sources else "backfill",
        "paper_sim_dir": str(paper_sim_dir),
        "simulation_sources": simulation_sources,
        "diagnosis_dir": str(diagnosis_dir),
        "diagnosis_sources": diagnosis_sources,
        "outcome_due_dir": str(outcome_due_dir),
        "due_sources": due_sources,
        "due_source_selection_status": (
            "INCLUDED" if include_outcome_due_sources else "EXCLUDED_POST_COMMITTED_UPDATE"
        ),
        "policy_path": str(policy_path),
        "policy_checksum": _file_sha256(policy_path),
        "policy": dict(policy),
        "production_effect": "none",
    }


def _latest_dashboard_source_bundle(
    *,
    root: Path,
    manifest_name: str,
    id_field: str,
    generated: datetime,
    validator: Any,
) -> list[dict[str, Any]]:
    manifest_paths = sorted(root.glob(f"*/{manifest_name}"))
    if not manifest_paths:
        return []
    candidates: list[tuple[datetime, str, dict[str, Any]]] = []
    eligible_seen = False
    for manifest_path in manifest_paths:
        manifest = _read_optional_json(manifest_path)
        if manifest is None:
            raise DynamicV3OutcomeAccumulationError(
                f"dashboard source manifest invalid: {manifest_path}"
            )
        source_time = _datetime_from_any(
            manifest.get("updated_at") or manifest.get("generated_at")
        )
        if source_time is None:
            raise DynamicV3OutcomeAccumulationError(
                f"dashboard source time missing: {manifest_path}"
            )
        if source_time > generated:
            continue
        eligible_seen = True
        artifact_id = _text(manifest.get(id_field), manifest_path.parent.name)
        validation = validator(artifact_id)
        if validation.get("status") != "PASS":
            continue
        candidates.append(
            (source_time, artifact_id, _immutable_source_bundle(manifest_path.parent))
        )
    if not candidates:
        if eligible_seen:
            raise DynamicV3OutcomeAccumulationError(
                f"no validated {manifest_name} source at dashboard cutoff"
            )
        return []
    candidates.sort(key=lambda row: (row[0], row[1]))
    latest_time = candidates[-1][0]
    latest = [row for row in candidates if row[0] == latest_time]
    if len(latest) != 1:
        raise DynamicV3OutcomeAccumulationError(
            f"ambiguous latest {manifest_name} source at dashboard cutoff"
        )
    return [latest[0][2]]


def _outcome_dashboard_rows_from_snapshot(
    snapshot: Mapping[str, Any],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    generated = _datetime_from_any(snapshot.get("generated_cutoff"))
    if generated is None:
        raise DynamicV3OutcomeAccumulationError("dashboard snapshot cutoff invalid")
    forward_rows: list[dict[str, Any]] = []
    identities: set[tuple[str, int]] = set()
    for bundle in _records(snapshot.get("forward_sources")):
        manifest = _mapping(_source_bundle_content(bundle, "advisory_outcome_manifest.json"))
        outcome_id = _text(manifest.get("outcome_id"))
        source_time = _datetime_from_any(manifest.get("updated_at") or manifest.get("generated_at"))
        if source_time is None or source_time > generated:
            raise DynamicV3OutcomeAccumulationError("forward outcome escaped dashboard cutoff")
        for row in _records(_source_bundle_content(bundle, "outcome_windows.jsonl")):
            identity = (outcome_id, _int(row.get("window_days")))
            if not outcome_id or identity[1] not in OUTCOME_WINDOWS or identity in identities:
                raise DynamicV3OutcomeAccumulationError(
                    "duplicate or invalid forward dashboard sample"
                )
            identities.add(identity)
            forward_rows.append(
                {
                    **row,
                    "source_mode": "FORWARD_OUTCOME",
                    "outcome_mode": "FORWARD_OUTCOME",
                    "outcome_id": outcome_id,
                    "source_artifact_path": _bundle_primary_path(
                        bundle, "advisory_outcome_manifest.json"
                    ),
                }
            )
    historical_rows: list[dict[str, Any]] = []
    historical_type = _text(snapshot.get("historical_source_type"))
    historical_filename = (
        "repaired_outcome_windows.jsonl"
        if historical_type == "repair"
        else "replay_outcome_windows.jsonl"
    )
    historical_manifest = (
        "backfill_repair_manifest.json"
        if historical_type == "repair"
        else "backfill_manifest.json"
    )
    historical_identities: set[tuple[str, str, int]] = set()
    for bundle in _records(snapshot.get("historical_sources")):
        for row in _records(_source_bundle_content(bundle, historical_filename)):
            identity = (
                _text(row.get("replay_event_id")),
                _text(row.get("variant")),
                _int(row.get("window_days")),
            )
            if not identity[0] or not identity[1] or identity[2] not in OUTCOME_WINDOWS:
                raise DynamicV3OutcomeAccumulationError("invalid historical dashboard sample")
            if identity in historical_identities:
                raise DynamicV3OutcomeAccumulationError("duplicate historical dashboard sample")
            historical_identities.add(identity)
            historical_rows.append(
                {
                    **row,
                    "source_mode": "HISTORICAL_REPLAY",
                    "outcome_mode": "HISTORICAL_REPLAY",
                    "source_artifact_path": _bundle_primary_path(bundle, historical_manifest),
                }
            )
    simulation_rows: list[dict[str, Any]] = []
    for bundle in _records(snapshot.get("simulation_sources")):
        manifest = _mapping(
            _source_bundle_content(bundle, "historical_paper_sim_manifest.json")
        )
        summary = _mapping(
            _source_bundle_content(bundle, "simulated_performance_summary.json")
        )
        status = _text(summary.get("simulation_status") or manifest.get("status"))
        outcome_status = "INSUFFICIENT_DATA"
        if status in {"PASS", "AVAILABLE"}:
            outcome_status = "AVAILABLE"
        elif status == "PENDING":
            outcome_status = "PENDING"
        simulation_rows.append(
            {
                "sample_id": _text(manifest.get("sim_id")),
                "source_mode": "BACKTEST_SIMULATION",
                "outcome_mode": "BACKTEST_SIMULATION",
                "window_days": 0,
                "outcome_status": outcome_status,
                "source_artifact_path": _bundle_primary_path(
                    bundle, "historical_paper_sim_manifest.json"
                ),
            }
        )
    pending_sources = {"diagnosis": [], "due": []}
    for bundle in _records(snapshot.get("diagnosis_sources")):
        summary = _mapping(
            _source_bundle_content(bundle, "replay_pending_reason_summary.json")
        )
        pending_sources["diagnosis"].extend(_records(summary.get("pending_reasons")))
    for bundle in _records(snapshot.get("due_sources")):
        pending_sources["due"].extend(
            _records(_source_bundle_content(bundle, "due_window_inventory.jsonl"))
        )
    return (
        {
            "FORWARD_OUTCOME": forward_rows,
            "HISTORICAL_REPLAY": historical_rows,
            "BACKTEST_SIMULATION": simulation_rows,
        },
        pending_sources,
    )


def _pending_reason_dashboard_from_rows(
    *,
    rows_by_mode: Mapping[str, Sequence[Mapping[str, Any]]],
    selected_sources: Mapping[str, Sequence[Mapping[str, Any]]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    reasons = Counter()
    for row in selected_sources.get("diagnosis", []):
        reasons[_text(row.get("reason"), "none")] += _int(row.get("count"), 0)
    for row in selected_sources.get("due", []):
        if _text(row.get("current_outcome_status")) == "PENDING":
            reasons[_text(row.get("reason"), "future_window_not_reached")] += 1
    for rows in rows_by_mode.values():
        for row in rows:
            if _text(row.get("outcome_status")) == "PENDING":
                reasons[_text(row.get("pending_reason"), "future_window_not_reached")] += 1
    if not reasons:
        reasons["none"] = 0
    actions = _mapping(policy.get("pending_reason_actions"))
    precedence = _texts(policy.get("pending_reason_precedence"))
    unknown = set(reasons) - set(actions)
    if unknown:
        raise DynamicV3OutcomeAccumulationError(
            f"pending reason is not governed by dashboard policy: {sorted(unknown)}"
        )
    top = [
        {"reason": reason, "count": reasons[reason], "action": _text(actions.get(reason))}
        for reason in precedence
        if reason in reasons
    ]
    actionable = [
        row for row in top if row["reason"] in {"missing_price_data", "review_waiting_for_backfill"}
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pending_reason_dashboard",
        "top_pending_reasons": top,
        "actionable_pending_reasons": actionable,
        "next_action": top[0]["action"] if top else "continue_forward_tracking",
        "policy_id": policy.get("policy_id"),
        "policy_version": policy.get("version"),
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _outcome_dashboard_live_source_validation(snapshot: Mapping[str, Any]) -> bool:
    forward_root = Path(_text(snapshot.get("advisory_outcome_dir")))
    for bundle in _records(snapshot.get("forward_sources")):
        manifest = _mapping(_source_bundle_content(bundle, "advisory_outcome_manifest.json"))
        if (
            validate_advisory_outcome_artifact(
                outcome_id=_text(manifest.get("outcome_id")), output_dir=forward_root
            ).get("status")
            != "PASS"
        ):
            return False
    historical_type = _text(snapshot.get("historical_source_type"))
    historical_root = Path(
        _text(snapshot.get("repair_dir" if historical_type == "repair" else "backfill_dir"))
    )
    for bundle in _records(snapshot.get("historical_sources")):
        if historical_type == "repair":
            manifest = _mapping(_source_bundle_content(bundle, "backfill_repair_manifest.json"))
            status = validate_backfill_repair_artifact(
                repair_id=_text(manifest.get("repair_id")), output_dir=historical_root
            ).get("status")
        else:
            manifest = _mapping(_source_bundle_content(bundle, "backfill_manifest.json"))
            status = validate_backfill_outcome_artifact(
                backfill_id=_text(manifest.get("backfill_id")), output_dir=historical_root
            ).get("status")
        if status != "PASS":
            return False
    sim_root = Path(_text(snapshot.get("paper_sim_dir")))
    for bundle in _records(snapshot.get("simulation_sources")):
        manifest = _mapping(
            _source_bundle_content(bundle, "historical_paper_sim_manifest.json")
        )
        if (
            validate_historical_paper_sim_artifact(
                sim_id=_text(manifest.get("sim_id")), output_dir=sim_root
            ).get("status")
            != "PASS"
        ):
            return False
    diagnosis_root = Path(_text(snapshot.get("diagnosis_dir")))
    for bundle in _records(snapshot.get("diagnosis_sources")):
        manifest = _mapping(_source_bundle_content(bundle, "replay_diagnosis_manifest.json"))
        if (
            validate_replay_diagnosis_artifact(
                diagnosis_id=_text(manifest.get("diagnosis_id")), output_dir=diagnosis_root
            ).get("status")
            != "PASS"
        ):
            return False
    due_root = Path(_text(snapshot.get("outcome_due_dir")))
    for bundle in _records(snapshot.get("due_sources")):
        manifest = _mapping(_source_bundle_content(bundle, "outcome_due_manifest.json"))
        if (
            validate_outcome_due_artifact(
                due_id=_text(manifest.get("due_id")), output_dir=due_root
            ).get("status")
            != "PASS"
        ):
            return False
    return True


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


def _load_limited_vs_notrade_policy(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise DynamicV3OutcomeAccumulationError(f"limited-vs-notrade policy not found: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    policy = dict(payload) if isinstance(payload, Mapping) else {}
    for key in (
        "schema_version",
        "policy_id",
        "version",
        "status",
        "owner",
        "rationale",
        "intended_effect",
        "validation_plan",
        "review_condition",
        "window_aggregation",
        "missing_regime_behavior",
    ):
        if not _text(policy.get(key)):
            raise DynamicV3OutcomeAccumulationError(
                "limited-vs-notrade policy metadata is incomplete"
            )
    windows = [_int(value) for value in _records_or_values(policy.get("tracked_windows"))]
    if tuple(windows) != OUTCOME_WINDOWS:
        raise DynamicV3OutcomeAccumulationError(
            "limited-vs-notrade tracked windows must match outcome contract"
        )
    medium = policy.get("medium_confidence_distinct_event_floor")
    high = policy.get("high_confidence_distinct_event_floor")
    if (
        isinstance(medium, bool)
        or isinstance(high, bool)
        or not isinstance(medium, int)
        or not isinstance(high, int)
        or not 0 < medium < high
    ):
        raise DynamicV3OutcomeAccumulationError(
            "limited-vs-notrade confidence floors are invalid"
        )
    if policy.get("window_aggregation") != "equal_window":
        raise DynamicV3OutcomeAccumulationError(
            "limited-vs-notrade window aggregation must be explicit equal_window"
        )
    regimes = _texts(policy.get("regime_taxonomy"))
    if not regimes or len(regimes) != len(set(regimes)):
        raise DynamicV3OutcomeAccumulationError("limited-vs-notrade regime taxonomy invalid")
    if policy.get("missing_regime_behavior") != "UNAVAILABLE":
        raise DynamicV3OutcomeAccumulationError(
            "limited-vs-notrade missing regime behavior must be UNAVAILABLE"
        )
    precedence = _texts(policy.get("recommendation_precedence"))
    if precedence != [
        "insufficient_data",
        "continue_tracking",
        "support_limited_adjustment",
        "weaken_limited_adjustment",
    ]:
        raise DynamicV3OutcomeAccumulationError(
            "limited-vs-notrade recommendation precedence invalid"
        )
    overall_threshold = policy.get("overall_relative_return_threshold")
    if isinstance(overall_threshold, bool) or not isinstance(overall_threshold, int | float):
        raise DynamicV3OutcomeAccumulationError(
            "limited-vs-notrade overall return threshold invalid"
        )
    if not math.isfinite(float(overall_threshold)):
        raise DynamicV3OutcomeAccumulationError(
            "limited-vs-notrade overall return threshold must be finite"
        )
    if policy.get("auto_policy_apply") is not False or policy.get("production_effect") != "none":
        raise DynamicV3OutcomeAccumulationError(
            "limited-vs-notrade policy safety boundary invalid"
        )
    policy["tracked_windows"] = windows
    policy["regime_taxonomy"] = regimes
    policy["recommendation_precedence"] = precedence
    return policy


def _records_or_values(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list | tuple) else []


def _build_limited_vs_notrade_snapshot(
    *,
    generated: datetime,
    advisory_outcome_dir: Path,
    backfill_dir: Path,
    repair_dir: Path,
    policy_path: Path,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    forward_sources: list[dict[str, Any]] = []
    ids: set[str] = set()
    for child in _artifact_children(advisory_outcome_dir):
        manifest = _read_optional_json(child / "advisory_outcome_manifest.json")
        if manifest is None:
            raise DynamicV3OutcomeAccumulationError(f"advisory outcome invalid: {child}")
        outcome_id = _text(manifest.get("outcome_id"), child.name)
        source_time = _datetime_from_any(manifest.get("updated_at") or manifest.get("generated_at"))
        if source_time is None or source_time > generated or outcome_id in ids:
            raise DynamicV3OutcomeAccumulationError(
                "advisory outcome time/id invalid for limited comparison"
            )
        if (
            validate_advisory_outcome_artifact(
                outcome_id=outcome_id, output_dir=advisory_outcome_dir
            ).get("status")
            != "PASS"
        ):
            raise DynamicV3OutcomeAccumulationError(
                f"advisory outcome validation must PASS: {outcome_id}"
            )
        ids.add(outcome_id)
        forward_sources.append(_immutable_source_bundle(child))
    repair_sources = _latest_dashboard_source_bundle(
        root=repair_dir,
        manifest_name="backfill_repair_manifest.json",
        id_field="repair_id",
        generated=generated,
        validator=lambda artifact_id: validate_backfill_repair_artifact(
            repair_id=artifact_id, output_dir=repair_dir
        ),
    )
    backfill_sources: list[dict[str, Any]] = []
    if not repair_sources:
        backfill_sources = _latest_dashboard_source_bundle(
            root=backfill_dir,
            manifest_name="backfill_manifest.json",
            id_field="backfill_id",
            generated=generated,
            validator=lambda artifact_id: validate_backfill_outcome_artifact(
                backfill_id=artifact_id, output_dir=backfill_dir
            ),
        )
    return {
        "schema_version": LIMITED_VS_NOTRADE_SNAPSHOT_SCHEMA_VERSION,
        "generated_at": generated.isoformat(),
        "generated_cutoff": generated.isoformat(),
        "advisory_outcome_dir": str(advisory_outcome_dir),
        "forward_sources": forward_sources,
        "backfill_dir": str(backfill_dir),
        "repair_dir": str(repair_dir),
        "historical_sources": repair_sources or backfill_sources,
        "historical_source_type": "repair" if repair_sources else "backfill",
        "policy_path": str(policy_path),
        "policy_checksum": _file_sha256(policy_path),
        "policy": dict(policy),
        "production_effect": "none",
    }


def _limited_vs_notrade_views_from_snapshot(
    snapshot: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    generated = _datetime_from_any(snapshot.get("generated_cutoff"))
    if generated is None:
        raise DynamicV3OutcomeAccumulationError("limited comparison snapshot cutoff invalid")
    samples: list[dict[str, Any]] = []
    source_row_count = 0
    unpaired_source_row_count = 0
    identities: set[tuple[str, int]] = set()
    for bundle in _records(snapshot.get("forward_sources")):
        manifest = _mapping(_source_bundle_content(bundle, "advisory_outcome_manifest.json"))
        event = _mapping(_source_bundle_content(bundle, "advisory_event.json"))
        outcome_id = _text(manifest.get("outcome_id"))
        source_time = _datetime_from_any(manifest.get("updated_at") or manifest.get("generated_at"))
        if source_time is None or source_time > generated:
            raise DynamicV3OutcomeAccumulationError("forward comparison source escaped cutoff")
        limited_supported = bool(_mapping(event.get("limited_adjustment_weights")))
        for row in _records(_source_bundle_content(bundle, "outcome_windows.jsonl")):
            source_row_count += 1
            window = _int(row.get("window_days"))
            identity = (outcome_id, window)
            if not outcome_id or window not in OUTCOME_WINDOWS or identity in identities:
                raise DynamicV3OutcomeAccumulationError(
                    "duplicate or invalid forward limited comparison sample"
                )
            identities.add(identity)
            status = _text(row.get("outcome_status"), "INSUFFICIENT_DATA")
            if status not in OUTCOME_WINDOW_STATUSES:
                raise DynamicV3OutcomeAccumulationError(
                    "forward comparison outcome status invalid"
                )
            limited_return = _optional_finite(row.get("limited_adjustment_return"))
            no_trade_return = _optional_finite(row.get("no_trade_return"))
            if not limited_supported:
                unpaired_source_row_count += 1
                continue
            if status == "AVAILABLE" and (limited_return is None or no_trade_return is None):
                raise DynamicV3OutcomeAccumulationError(
                    "AVAILABLE forward comparison sample missing finite paired return"
                )
            samples.append(
                _limited_sample_row(
                    sample_id=_stable_id("limited-forward", outcome_id, str(window)),
                    source_mode="FORWARD_OUTCOME",
                    as_of=_text(row.get("start_date") or manifest.get("as_of")),
                    window=window,
                    limited_return=limited_return,
                    no_trade_return=no_trade_return,
                    limited_drawdown=None,
                    no_trade_drawdown=None,
                    turnover=None,
                    status=status,
                    regime=_text(row.get("regime")),
                )
            )
    historical_type = _text(snapshot.get("historical_source_type"))
    if historical_type not in {"repair", "backfill"}:
        raise DynamicV3OutcomeAccumulationError(
            "limited comparison historical source type invalid"
        )
    filename = (
        "repaired_outcome_windows.jsonl"
        if historical_type == "repair"
        else "replay_outcome_windows.jsonl"
    )
    grouped: dict[tuple[str, int], dict[str, Mapping[str, Any]]] = defaultdict(dict)
    for bundle in _records(snapshot.get("historical_sources")):
        for row in _records(_source_bundle_content(bundle, filename)):
            source_row_count += 1
            key = (
                _text(row.get("replay_event_id") or row.get("daily_advisory_id")),
                _int(row.get("window_days")),
            )
            variant = _text(row.get("variant"))
            status = _text(row.get("outcome_status"))
            if (
                not key[0]
                or key[1] not in OUTCOME_WINDOWS
                or variant not in {"limited_adjustment", "no_trade"}
                or status not in OUTCOME_WINDOW_STATUSES
            ):
                raise DynamicV3OutcomeAccumulationError("invalid historical comparison identity")
            if variant in grouped[key]:
                raise DynamicV3OutcomeAccumulationError(
                    "duplicate historical event/window/variant comparison row"
                )
            grouped[key][variant] = row
    for (event_id, window), variants in sorted(grouped.items()):
        limited = variants.get("limited_adjustment")
        no_trade = variants.get("no_trade")
        if limited is None or no_trade is None:
            unpaired_source_row_count += len(variants)
            continue
        status = _paired_status(limited, no_trade)
        limited_return = _optional_finite(limited.get("return"))
        no_trade_return = _optional_finite(no_trade.get("return"))
        if status == "AVAILABLE" and (limited_return is None or no_trade_return is None):
            raise DynamicV3OutcomeAccumulationError(
                "AVAILABLE historical comparison sample missing finite paired return"
            )
        limited_regime = _text(limited.get("regime"))
        no_trade_regime = _text(no_trade.get("regime"))
        if limited_regime and no_trade_regime and limited_regime != no_trade_regime:
            raise DynamicV3OutcomeAccumulationError("paired comparison regime labels conflict")
        samples.append(
            _limited_sample_row(
                sample_id=_stable_id("limited-replay", event_id, str(window)),
                source_mode="HISTORICAL_REPLAY",
                as_of=_text(limited.get("as_of")),
                window=window,
                limited_return=limited_return,
                no_trade_return=no_trade_return,
                limited_drawdown=_optional_finite(limited.get("max_drawdown")),
                no_trade_drawdown=_optional_finite(no_trade.get("max_drawdown")),
                turnover=_optional_finite(limited.get("turnover")),
                status=status,
                regime=limited_regime or no_trade_regime,
            )
        )
    samples.sort(key=lambda row: (_text(row.get("as_of")), _text(row.get("sample_id"))))
    coverage = {
        "source_row_count": source_row_count,
        "paired_sample_count": len(samples),
        "unpaired_source_row_count": unpaired_source_row_count,
        "available_paired_sample_count": sum(
            1 for row in samples if row.get("sample_status") == "AVAILABLE"
        ),
        "production_effect": "none",
    }
    return samples, coverage


def _optional_finite(value: Any) -> float | None:
    if value is None or value == "":
        return None
    result = _float(value, default=float("nan"))
    return result if math.isfinite(result) else None


def _limited_sample_row(
    *,
    sample_id: str,
    source_mode: str,
    as_of: str,
    window: int,
    limited_return: float | None,
    no_trade_return: float | None,
    limited_drawdown: float | None,
    no_trade_drawdown: float | None,
    turnover: float | None,
    status: str,
    regime: str,
) -> dict[str, Any]:
    available = status == "AVAILABLE"
    return {
        "sample_id": sample_id,
        "source_mode": source_mode,
        "as_of": as_of,
        "window_days": window,
        "limited_adjustment_return": limited_return if available else None,
        "no_trade_return": no_trade_return if available else None,
        "relative_return": (
            round(limited_return - no_trade_return, 6)
            if available and limited_return is not None and no_trade_return is not None
            else None
        ),
        "limited_drawdown": limited_drawdown if available else None,
        "no_trade_drawdown": no_trade_drawdown if available else None,
        "relative_drawdown": (
            round(limited_drawdown - no_trade_drawdown, 6)
            if available and limited_drawdown is not None and no_trade_drawdown is not None
            else None
        ),
        "turnover": turnover if available else None,
        "sample_status": status if status in OUTCOME_WINDOW_STATUSES else "INSUFFICIENT_DATA",
        "regime": regime or None,
    }


def _limited_window_metrics_v2(
    samples: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]
) -> list[dict[str, Any]]:
    result = []
    medium = _int(policy.get("medium_confidence_distinct_event_floor"))
    high = _int(policy.get("high_confidence_distinct_event_floor"))
    win_threshold = _float(policy.get("win_relative_return_threshold"))
    for window in _records_or_values(policy.get("tracked_windows")):
        rows = [
            row
            for row in samples
            if _int(row.get("window_days")) == _int(window)
            and row.get("sample_status") == "AVAILABLE"
        ]
        rel = [
            value
            for row in rows
            if (value := _optional_finite(row.get("relative_return"))) is not None
        ]
        drawdowns = [
            value
            for row in rows
            if (value := _optional_finite(row.get("relative_drawdown"))) is not None
        ]
        turnovers = [
            value for row in rows if (value := _optional_finite(row.get("turnover"))) is not None
        ]
        count = len(rows)
        confidence = "INSUFFICIENT_DATA"
        if count >= high:
            confidence = "HIGH"
        elif count >= medium:
            confidence = "MEDIUM"
        elif count > 0:
            confidence = "LOW"
        result.append(
            {
                "window_days": _int(window),
                "available_count": count,
                "distinct_event_count": count,
                "avg_relative_return": round(_avg(rel), 6) if rel else None,
                "median_relative_return": round(_median(rel), 6) if rel else None,
                "win_rate": (
                    round(sum(1 for value in rel if value > win_threshold) / len(rel), 6)
                    if rel
                    else None
                ),
                "avg_drawdown_delta": round(_avg(drawdowns), 6) if drawdowns else None,
                "avg_turnover": round(_avg(turnovers), 6) if turnovers else None,
                "confidence": confidence,
            }
        )
    return result


def _limited_overall_recommendation_v2(
    metrics: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]
) -> str:
    if policy.get("window_aggregation") != "equal_window":
        raise DynamicV3OutcomeAccumulationError("unsupported limited comparison aggregation")
    available = [row for row in metrics if _int(row.get("available_count")) > 0]
    if not available:
        return "insufficient_data"
    confident = [row for row in available if _text(row.get("confidence")) in {"MEDIUM", "HIGH"}]
    if not confident:
        return "continue_tracking"
    values = [
        value
        for row in confident
        if (value := _optional_finite(row.get("avg_relative_return"))) is not None
    ]
    if not values:
        return "insufficient_data"
    threshold = _float(policy.get("overall_relative_return_threshold"))
    return (
        "support_limited_adjustment"
        if _avg(values) > threshold
        else "weaken_limited_adjustment"
    )


def _limited_regime_breakdown_v2(
    samples: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]
) -> dict[str, Any]:
    regimes = _texts(policy.get("regime_taxonomy"))
    unexpected = sorted(
        {
            label
            for row in samples
            if row.get("sample_status") == "AVAILABLE"
            and (label := _text(row.get("regime")))
            and label not in regimes
        }
    )
    if unexpected:
        raise DynamicV3OutcomeAccumulationError(
            f"limited comparison regime labels outside policy taxonomy: {unexpected}"
        )
    by_regime: dict[str, Any] = {}
    labeled_count = 0
    for regime in regimes:
        rows = [
            row
            for row in samples
            if row.get("sample_status") == "AVAILABLE" and _text(row.get("regime")) == regime
        ]
        values = [
            value
            for row in rows
            if (value := _optional_finite(row.get("relative_return"))) is not None
        ]
        labeled_count += len(rows)
        by_regime[regime] = {
            "available_count": len(rows),
            "avg_relative_return": round(_avg(values), 6) if values else None,
            "status": "AVAILABLE" if rows else "INSUFFICIENT_DATA",
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_limited_vs_notrade_regime_breakdown",
        "status": "PARTIAL" if labeled_count else _text(policy.get("missing_regime_behavior")),
        "labeled_sample_count": labeled_count,
        "by_regime": by_regime,
        "policy_id": policy.get("policy_id"),
        "policy_version": policy.get("version"),
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _limited_vs_notrade_live_source_validation(snapshot: Mapping[str, Any]) -> bool:
    forward_root = Path(_text(snapshot.get("advisory_outcome_dir")))
    for bundle in _records(snapshot.get("forward_sources")):
        manifest = _mapping(_source_bundle_content(bundle, "advisory_outcome_manifest.json"))
        if (
            validate_advisory_outcome_artifact(
                outcome_id=_text(manifest.get("outcome_id")), output_dir=forward_root
            ).get("status")
            != "PASS"
        ):
            return False
    historical_type = _text(snapshot.get("historical_source_type"))
    root = Path(
        _text(snapshot.get("repair_dir" if historical_type == "repair" else "backfill_dir"))
    )
    for bundle in _records(snapshot.get("historical_sources")):
        if historical_type == "repair":
            manifest = _mapping(_source_bundle_content(bundle, "backfill_repair_manifest.json"))
            status = validate_backfill_repair_artifact(
                repair_id=_text(manifest.get("repair_id")), output_dir=root
            ).get("status")
        else:
            manifest = _mapping(_source_bundle_content(bundle, "backfill_manifest.json"))
            status = validate_backfill_outcome_artifact(
                backfill_id=_text(manifest.get("backfill_id")), output_dir=root
            ).get("status")
        if status != "PASS":
            return False
    return True


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


def _load_consensus_risk_policy(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise DynamicV3OutcomeAccumulationError(f"consensus risk policy not found: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    policy = dict(payload) if isinstance(payload, Mapping) else {}
    required_text = (
        "schema_version",
        "policy_id",
        "version",
        "status",
        "owner",
        "rationale",
        "intended_effect",
        "validation_plan",
        "review_condition",
        "exposure_sample_identity",
        "daily_replay_overlap_behavior",
    )
    if any(not _text(policy.get(key)) for key in required_text):
        raise DynamicV3OutcomeAccumulationError("consensus risk policy metadata is incomplete")
    windows = [_int(value) for value in _records_or_values(policy.get("tracked_windows"))]
    required_windows = [
        _int(value)
        for value in _records_or_values(policy.get("required_drawdown_windows_for_pass"))
    ]
    if tuple(windows) != OUTCOME_WINDOWS or not required_windows or not set(
        required_windows
    ).issubset(windows):
        raise DynamicV3OutcomeAccumulationError("consensus risk window policy invalid")
    for key in (
        "minimum_distinct_exposure_samples",
        "minimum_distinct_drawdown_pairs_per_window",
    ):
        value = policy.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise DynamicV3OutcomeAccumulationError(f"consensus risk sample floor invalid: {key}")
    numeric_keys = (
        "semiconductor_exposure_review_level",
        "risk_asset_exposure_review_level",
        "drawdown_delta_review_threshold",
        "average_turnover_review_level",
        "single_sample_turnover_warning_level",
        "single_sample_turnover_review_level",
    )
    if any(
        isinstance(policy.get(key), bool)
        or not isinstance(policy.get(key), int | float)
        or not math.isfinite(float(policy[key]))
        for key in numeric_keys
    ):
        raise DynamicV3OutcomeAccumulationError("consensus risk thresholds must be finite numbers")
    if not (
        0 <= _float(policy.get("semiconductor_exposure_review_level")) <= 1
        and 0 <= _float(policy.get("risk_asset_exposure_review_level")) <= 1
        and _float(policy.get("drawdown_delta_review_threshold")) <= 0
        and 0
        <= _float(policy.get("single_sample_turnover_warning_level"))
        <= _float(policy.get("single_sample_turnover_review_level"))
        and 0 <= _float(policy.get("average_turnover_review_level"))
    ):
        raise DynamicV3OutcomeAccumulationError("consensus risk threshold ordering invalid")
    precedence = _texts(policy.get("overall_status_precedence"))
    if precedence != [
        "REVIEW_REQUIRED",
        "INSUFFICIENT_DATA",
        "PASS_WITH_WARNINGS",
        "PASS",
    ]:
        raise DynamicV3OutcomeAccumulationError("consensus risk status precedence invalid")
    if (
        policy.get("exposure_sample_identity") != "decision_as_of"
        or policy.get("daily_replay_overlap_behavior") != "require_equal_then_merge"
        or policy.get("consensus_target_default_execution_recommended") is not False
        or policy.get("auto_policy_apply") is not False
        or policy.get("production_effect") != "none"
    ):
        raise DynamicV3OutcomeAccumulationError("consensus risk policy safety boundary invalid")
    policy["tracked_windows"] = windows
    policy["required_drawdown_windows_for_pass"] = required_windows
    policy["overall_status_precedence"] = precedence
    return policy


def _build_consensus_risk_snapshot(
    *,
    generated: datetime,
    daily_advisory_dir: Path,
    historical_replay_dir: Path,
    backfill_dir: Path,
    repair_dir: Path,
    policy_path: Path,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    daily_sources: list[dict[str, Any]] = []
    daily_ids: set[str] = set()
    daily_dates: set[str] = set()
    for child in _artifact_children(daily_advisory_dir):
        manifest = _read_optional_json(child / "daily_advisory_manifest.json")
        if manifest is None:
            raise DynamicV3OutcomeAccumulationError(f"daily advisory invalid: {child}")
        advisory_id = _text(manifest.get("daily_advisory_id"), child.name)
        as_of = _text(manifest.get("as_of"))
        if (
            not advisory_id
            or not as_of
            or not _consensus_daily_time_within_cutoff(manifest, generated)
            or advisory_id in daily_ids
            or as_of in daily_dates
        ):
            raise DynamicV3OutcomeAccumulationError(
                "daily advisory time/id/as-of invalid for consensus risk"
            )
        if (
            validate_position_advisory_daily_artifact(
                daily_advisory_id=advisory_id, output_dir=daily_advisory_dir
            ).get("status")
            != "PASS"
        ):
            raise DynamicV3OutcomeAccumulationError(
                f"daily advisory validation must PASS: {advisory_id}"
            )
        daily_ids.add(advisory_id)
        daily_dates.add(as_of)
        daily_sources.append(_immutable_source_bundle(child))
    replay_sources = _latest_dashboard_source_bundle(
        root=historical_replay_dir,
        manifest_name="historical_replay_manifest.json",
        id_field="replay_id",
        generated=generated,
        validator=lambda artifact_id: validate_historical_replay_artifact(
            replay_id=artifact_id, output_dir=historical_replay_dir
        ),
    )
    repair_sources = _latest_dashboard_source_bundle(
        root=repair_dir,
        manifest_name="backfill_repair_manifest.json",
        id_field="repair_id",
        generated=generated,
        validator=lambda artifact_id: validate_backfill_repair_artifact(
            repair_id=artifact_id, output_dir=repair_dir
        ),
    )
    backfill_sources: list[dict[str, Any]] = []
    if not repair_sources:
        backfill_sources = _latest_dashboard_source_bundle(
            root=backfill_dir,
            manifest_name="backfill_manifest.json",
            id_field="backfill_id",
            generated=generated,
            validator=lambda artifact_id: validate_backfill_outcome_artifact(
                backfill_id=artifact_id, output_dir=backfill_dir
            ),
        )
    outcome_sources = repair_sources or backfill_sources
    if outcome_sources and not replay_sources:
        raise DynamicV3OutcomeAccumulationError(
            "consensus risk outcome source requires selected historical replay lineage"
        )
    if outcome_sources:
        replay_manifest = _mapping(
            _source_bundle_content(replay_sources[0], "historical_replay_manifest.json")
        )
        outcome_manifest_name = (
            "backfill_repair_manifest.json" if repair_sources else "backfill_manifest.json"
        )
        outcome_manifest = _mapping(
            _source_bundle_content(outcome_sources[0], outcome_manifest_name)
        )
        if _text(replay_manifest.get("replay_id")) != _text(
            outcome_manifest.get("replay_id")
        ):
            raise DynamicV3OutcomeAccumulationError(
                "consensus risk replay and outcome lineage mismatch"
            )
    return {
        "schema_version": CONSENSUS_RISK_SNAPSHOT_SCHEMA_VERSION,
        "generated_at": generated.isoformat(),
        "generated_cutoff": generated.isoformat(),
        "daily_advisory_dir": str(daily_advisory_dir),
        "daily_sources": daily_sources,
        "historical_replay_dir": str(historical_replay_dir),
        "historical_replay_sources": replay_sources,
        "backfill_dir": str(backfill_dir),
        "repair_dir": str(repair_dir),
        "outcome_source_type": "repair" if repair_sources else "backfill",
        "outcome_sources": outcome_sources,
        "policy_path": str(policy_path),
        "policy_checksum": _file_sha256(policy_path),
        "policy": dict(policy),
        "production_effect": "none",
    }


def _consensus_weights_from_daily_bundle(bundle: Mapping[str, Any]) -> dict[str, float]:
    content = _source_bundle_content(bundle, "daily_consensus_weights.csv")
    if not isinstance(content, str) or not content.strip():
        return {}
    weights: dict[str, float] = {}
    for row in csv.DictReader(io.StringIO(content)):
        symbol = _text(row.get("symbol"))
        value = _optional_finite(row.get("median_target_weight"))
        if symbol and value is not None:
            if symbol in weights:
                raise DynamicV3OutcomeAccumulationError(
                    "duplicate daily consensus weight symbol"
                )
            weights[symbol] = value
    return _validated_consensus_weights(weights)


def _consensus_daily_time_within_cutoff(
    manifest: Mapping[str, Any], generated: datetime
) -> bool:
    explicit = manifest.get("updated_at") or manifest.get("generated_at")
    if explicit:
        parsed = _datetime_from_any(explicit)
        return parsed is not None and parsed <= generated
    as_of = _date_from_any(manifest.get("as_of"))
    return as_of is not None and as_of <= generated.date()


def _validated_consensus_weights(weights: Mapping[str, Any]) -> dict[str, float]:
    parsed: dict[str, float] = {}
    for symbol, raw in weights.items():
        value = _optional_finite(raw)
        if not _text(symbol) or value is None or value < 0:
            raise DynamicV3OutcomeAccumulationError("consensus weights contain invalid value")
        parsed[_text(symbol)] = value
    if not parsed:
        return {}
    if "CASH" not in parsed or abs(sum(parsed.values()) - 1.0) > 1e-6:
        raise DynamicV3OutcomeAccumulationError(
            "consensus weights require explicit CASH and unit simplex"
        )
    return {key: round(value, 6) for key, value in sorted(parsed.items())}


def _daily_turnover_from_bundle(bundle: Mapping[str, Any]) -> float | None:
    values: list[float] = []
    for row in _records(_source_bundle_content(bundle, "daily_position_deltas.jsonl")):
        deltas = _mapping(row.get("deltas"))
        if not deltas:
            continue
        parsed = [_optional_finite(value) for value in deltas.values()]
        if any(value is None for value in parsed):
            raise DynamicV3OutcomeAccumulationError("daily turnover delta is non-finite")
        values.append(sum(abs(value) for value in parsed if value is not None))
    return round(max(values), 6) if values else None


def _merge_consensus_exposure_sample(
    by_as_of: dict[str, dict[str, Any]], sample: Mapping[str, Any]
) -> None:
    as_of = _text(sample.get("as_of"))
    existing = by_as_of.get(as_of)
    if existing is None:
        by_as_of[as_of] = dict(sample)
        return
    if existing.get("weights") != sample.get("weights"):
        raise DynamicV3OutcomeAccumulationError(
            "daily and replay consensus weights conflict for one decision date"
        )
    left_turnover = _optional_finite(existing.get("turnover"))
    right_turnover = _optional_finite(sample.get("turnover"))
    if (
        left_turnover is not None
        and right_turnover is not None
        and abs(left_turnover - right_turnover) > 1e-6
    ):
        raise DynamicV3OutcomeAccumulationError(
            "daily and replay turnover conflict for one decision date"
        )
    existing["turnover"] = (
        left_turnover if left_turnover is not None else right_turnover
    )
    existing["source_mode"] = "DAILY_AND_HISTORICAL_REPLAY"
    existing["source_ids"] = sorted(
        set(_texts(existing.get("source_ids"))) | set(_texts(sample.get("source_ids")))
    )


def _consensus_risk_views_from_snapshot(
    snapshot: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    generated = _datetime_from_any(snapshot.get("generated_cutoff"))
    if generated is None:
        raise DynamicV3OutcomeAccumulationError("consensus risk snapshot cutoff invalid")
    by_as_of: dict[str, dict[str, Any]] = {}
    daily_row_count = 0
    replay_row_count = 0
    missing_consensus_weight_count = 0
    for bundle in _records(snapshot.get("daily_sources")):
        manifest = _mapping(_source_bundle_content(bundle, "daily_advisory_manifest.json"))
        advisory_id = _text(manifest.get("daily_advisory_id"))
        as_of = _text(manifest.get("as_of"))
        if (
            not advisory_id
            or not as_of
            or not _consensus_daily_time_within_cutoff(manifest, generated)
        ):
            raise DynamicV3OutcomeAccumulationError("daily exposure escaped consensus cutoff")
        daily_row_count += 1
        weights = _consensus_weights_from_daily_bundle(bundle)
        if not weights:
            missing_consensus_weight_count += 1
            continue
        _merge_consensus_exposure_sample(
            by_as_of,
            {
                "sample_id": _stable_id("consensus-exposure", as_of),
                "source_mode": "DAILY_ADVISORY",
                "source_ids": [advisory_id],
                "as_of": as_of,
                "weights": weights,
                "turnover": _daily_turnover_from_bundle(bundle),
            },
        )
    replay_event_ids: set[str] = set()
    replay_dates: set[str] = set()
    for bundle in _records(snapshot.get("historical_replay_sources")):
        for event in _records(_source_bundle_content(bundle, "replay_events.jsonl")):
            replay_row_count += 1
            event_id = _text(event.get("replay_event_id") or event.get("daily_advisory_id"))
            as_of = _text(event.get("as_of"))
            if (
                not event_id
                or not as_of
                or event_id in replay_event_ids
                or as_of in replay_dates
            ):
                raise DynamicV3OutcomeAccumulationError(
                    "duplicate or invalid replay consensus exposure identity"
                )
            replay_event_ids.add(event_id)
            replay_dates.add(as_of)
            variants = [
                row
                for row in _records(event.get("variants"))
                if _text(row.get("variant")) == "consensus_target"
            ]
            if len(variants) > 1:
                raise DynamicV3OutcomeAccumulationError(
                    "duplicate consensus_target variant in replay event"
                )
            if not variants:
                missing_consensus_weight_count += 1
                continue
            weights = _validated_consensus_weights(_mapping(variants[0].get("weights")))
            if not weights:
                missing_consensus_weight_count += 1
                continue
            turnover = _optional_finite(variants[0].get("turnover"))
            if turnover is not None and turnover < 0:
                raise DynamicV3OutcomeAccumulationError("replay turnover cannot be negative")
            _merge_consensus_exposure_sample(
                by_as_of,
                {
                    "sample_id": _stable_id("consensus-exposure", as_of),
                    "source_mode": "HISTORICAL_REPLAY",
                    "source_ids": [event_id],
                    "as_of": as_of,
                    "weights": weights,
                    "turnover": round(turnover, 6) if turnover is not None else None,
                },
            )
    outcome_type = _text(snapshot.get("outcome_source_type"))
    if outcome_type not in {"repair", "backfill"}:
        raise DynamicV3OutcomeAccumulationError("consensus outcome source type invalid")
    outcome_filename = (
        "repaired_outcome_windows.jsonl"
        if outcome_type == "repair"
        else "replay_outcome_windows.jsonl"
    )
    grouped: dict[tuple[str, int], dict[str, Mapping[str, Any]]] = defaultdict(dict)
    outcome_row_count = 0
    for bundle in _records(snapshot.get("outcome_sources")):
        for row in _records(_source_bundle_content(bundle, outcome_filename)):
            outcome_row_count += 1
            variant = _text(row.get("variant"))
            if variant not in {"consensus_target", "no_trade"}:
                continue
            key = (
                _text(row.get("replay_event_id") or row.get("daily_advisory_id")),
                _int(row.get("window_days")),
            )
            if not key[0] or key[1] not in OUTCOME_WINDOWS:
                raise DynamicV3OutcomeAccumulationError(
                    "consensus outcome comparison identity invalid"
                )
            if variant in grouped[key]:
                raise DynamicV3OutcomeAccumulationError(
                    "duplicate consensus outcome event/window/variant"
                )
            status = _text(row.get("outcome_status"))
            if status not in OUTCOME_WINDOW_STATUSES:
                raise DynamicV3OutcomeAccumulationError("consensus outcome status invalid")
            grouped[key][variant] = row
    outcome_pairs: list[dict[str, Any]] = []
    unpaired_outcome_row_count = 0
    for (event_id, window), variants in sorted(grouped.items()):
        consensus = variants.get("consensus_target")
        no_trade = variants.get("no_trade")
        if consensus is None or no_trade is None:
            unpaired_outcome_row_count += len(variants)
            continue
        status = _paired_status(consensus, no_trade)
        consensus_drawdown = _optional_finite(consensus.get("max_drawdown"))
        no_trade_drawdown = _optional_finite(no_trade.get("max_drawdown"))
        if status == "AVAILABLE" and (
            consensus_drawdown is None or no_trade_drawdown is None
        ):
            raise DynamicV3OutcomeAccumulationError(
                "AVAILABLE consensus drawdown pair missing finite metrics"
            )
        as_of_left = _text(consensus.get("as_of"))
        as_of_right = _text(no_trade.get("as_of"))
        if as_of_left and as_of_right and as_of_left != as_of_right:
            raise DynamicV3OutcomeAccumulationError("consensus outcome pair as-of conflict")
        outcome_pairs.append(
            {
                "pair_id": _stable_id("consensus-drawdown", event_id, str(window)),
                "event_id": event_id,
                "as_of": as_of_left or as_of_right or None,
                "window_days": window,
                "sample_status": status,
                "consensus_drawdown": (
                    consensus_drawdown if status == "AVAILABLE" else None
                ),
                "no_trade_drawdown": no_trade_drawdown if status == "AVAILABLE" else None,
                "drawdown_delta_vs_no_trade": (
                    round(consensus_drawdown - no_trade_drawdown, 6)
                    if status == "AVAILABLE"
                    and consensus_drawdown is not None
                    and no_trade_drawdown is not None
                    else None
                ),
            }
        )
    samples = sorted(by_as_of.values(), key=lambda row: _text(row.get("as_of")))
    coverage = {
        "daily_source_row_count": daily_row_count,
        "historical_replay_event_count": replay_row_count,
        "distinct_exposure_sample_count": len(samples),
        "merged_daily_replay_date_count": sum(
            1 for row in samples if row.get("source_mode") == "DAILY_AND_HISTORICAL_REPLAY"
        ),
        "missing_consensus_weight_count": missing_consensus_weight_count,
        "outcome_source_row_count": outcome_row_count,
        "paired_outcome_sample_count": len(outcome_pairs),
        "available_paired_outcome_sample_count": sum(
            1 for row in outcome_pairs if row.get("sample_status") == "AVAILABLE"
        ),
        "unpaired_outcome_row_count": unpaired_outcome_row_count,
        "production_effect": "none",
    }
    return samples, coverage, outcome_pairs


def _nullable_min_mean_max(values: Sequence[float]) -> dict[str, float | None]:
    if not values:
        return {"min": None, "mean": None, "max": None}
    return {
        "min": round(min(values), 6),
        "mean": round(_avg(values), 6),
        "max": round(max(values), 6),
    }


def _consensus_exposure_summary_v2(
    samples: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
    coverage: Mapping[str, Any],
) -> dict[str, Any]:
    risk_asset: list[float] = []
    cash: list[float] = []
    semiconductor: list[float] = []
    warnings: set[str] = set()
    semi_level = _float(policy.get("semiconductor_exposure_review_level"))
    risk_level = _float(policy.get("risk_asset_exposure_review_level"))
    for sample in samples:
        weights = _mapping(sample.get("weights"))
        cash_weight = _optional_finite(weights.get("CASH"))
        if cash_weight is None:
            raise DynamicV3OutcomeAccumulationError("consensus exposure missing CASH")
        risk_asset_weight = round(1.0 - cash_weight, 6)
        semi_weight = round(
            sum(_float(weights.get(symbol)) for symbol in SEMICONDUCTOR_SYMBOLS), 6
        )
        cash.append(cash_weight)
        risk_asset.append(risk_asset_weight)
        semiconductor.append(semi_weight)
        if semi_weight > semi_level:
            warnings.add("semiconductor_exposure_review_required")
        if risk_asset_weight > risk_level:
            warnings.add("risk_asset_exposure_review_required")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_consensus_exposure_summary",
        "sample_count": len(samples),
        "sample_identity": policy.get("exposure_sample_identity"),
        "risk_asset_exposure": _nullable_min_mean_max(risk_asset),
        "cash_exposure": _nullable_min_mean_max(cash),
        "semiconductor_exposure": _nullable_min_mean_max(semiconductor),
        "concentration_warnings": sorted(warnings),
        "coverage": dict(coverage),
        "policy_id": policy.get("policy_id"),
        "policy_version": policy.get("version"),
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _consensus_drawdown_risk_v2(
    pairs: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]
) -> dict[str, Any]:
    floor = _int(policy.get("minimum_distinct_drawdown_pairs_per_window"))
    threshold = _float(policy.get("drawdown_delta_review_threshold"))
    required_windows = set(_records_or_values(policy.get("required_drawdown_windows_for_pass")))
    results: list[dict[str, Any]] = []
    for window in _records_or_values(policy.get("tracked_windows")):
        rows = [
            row
            for row in pairs
            if _int(row.get("window_days")) == _int(window)
            and row.get("sample_status") == "AVAILABLE"
        ]
        drawdowns = [
            value
            for row in rows
            if (value := _optional_finite(row.get("consensus_drawdown"))) is not None
        ]
        deltas = [
            value
            for row in rows
            if (value := _optional_finite(row.get("drawdown_delta_vs_no_trade"))) is not None
        ]
        if len(drawdowns) != len(rows) or len(deltas) != len(rows):
            raise DynamicV3OutcomeAccumulationError("available drawdown pair projection invalid")
        status = "INSUFFICIENT_DATA"
        delta = round(_avg(deltas), 6) if deltas else None
        if len(rows) >= floor:
            status = "REVIEW_REQUIRED" if delta is not None and delta < threshold else "PASS"
        results.append(
            {
                "window_days": _int(window),
                "required_for_pass": _int(window) in required_windows,
                "available_count": len(rows),
                "distinct_event_count": len(rows),
                "avg_drawdown": round(_avg(drawdowns), 6) if drawdowns else None,
                "max_drawdown": round(min(drawdowns), 6) if drawdowns else None,
                "drawdown_delta_vs_no_trade": delta,
                "risk_status": status,
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_consensus_drawdown_risk",
        "window_results": results,
        "required_windows": sorted(required_windows),
        "policy_id": policy.get("policy_id"),
        "policy_version": policy.get("version"),
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _consensus_turnover_risk_v2(
    exposure_samples: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]
) -> dict[str, Any]:
    values = [
        value
        for row in exposure_samples
        if (value := _optional_finite(row.get("turnover"))) is not None
    ]
    if any(value < 0 for value in values):
        raise DynamicV3OutcomeAccumulationError("consensus turnover cannot be negative")
    avg_turnover = round(_avg(values), 6) if values else None
    max_turnover = round(max(values), 6) if values else None
    warnings: list[str] = []
    status = "INSUFFICIENT_DATA"
    floor = _int(policy.get("minimum_distinct_exposure_samples"))
    if len(values) >= floor:
        if (
            avg_turnover is not None
            and avg_turnover > _float(policy.get("average_turnover_review_level"))
        ) or (
            max_turnover is not None
            and max_turnover > _float(policy.get("single_sample_turnover_review_level"))
        ):
            status = "REVIEW_REQUIRED"
            warnings.append("turnover_review_required")
        elif max_turnover is not None and max_turnover > _float(
            policy.get("single_sample_turnover_warning_level")
        ):
            status = "PASS_WITH_WARNINGS"
            warnings.append("high_single_sample_turnover")
        else:
            status = "PASS"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_consensus_turnover_risk",
        "sample_count": len(values),
        "sample_identity": policy.get("exposure_sample_identity"),
        "avg_turnover": avg_turnover,
        "max_turnover": max_turnover,
        "turnover_status": status,
        "warnings": warnings,
        "policy_id": policy.get("policy_id"),
        "policy_version": policy.get("version"),
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _consensus_overall_risk_status_v2(
    exposure: Mapping[str, Any],
    drawdown: Mapping[str, Any],
    turnover: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> str:
    rows = _records(drawdown.get("window_results"))
    statuses = [_text(row.get("risk_status")) for row in rows]
    statuses.append(_text(turnover.get("turnover_status")))
    if "REVIEW_REQUIRED" in statuses:
        return "REVIEW_REQUIRED"
    required_rows = [row for row in rows if row.get("required_for_pass") is True]
    evidence_complete = (
        _int(exposure.get("sample_count"))
        >= _int(policy.get("minimum_distinct_exposure_samples"))
        and _text(turnover.get("turnover_status"))
        in {"PASS", "PASS_WITH_WARNINGS"}
        and bool(required_rows)
        and all(row.get("risk_status") == "PASS" for row in required_rows)
    )
    if not evidence_complete:
        return "INSUFFICIENT_DATA"
    if _texts(exposure.get("concentration_warnings")) or "PASS_WITH_WARNINGS" in statuses:
        return "PASS_WITH_WARNINGS"
    return "PASS"


def _consensus_risk_manifest(
    *,
    risk_dir: Path,
    generated_at: str,
    risk_status: str,
    exposure: Mapping[str, Any],
    drawdown: Mapping[str, Any],
    coverage: Mapping[str, Any],
    policy: Mapping[str, Any],
    source_snapshot_checksum: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_consensus_risk_manifest",
        "risk_id": risk_dir.name,
        "generated_at": generated_at,
        "status": risk_status,
        "consensus_target_risk": risk_status,
        "sample_count": exposure.get("sample_count"),
        "paired_outcome_sample_count": coverage.get("paired_outcome_sample_count"),
        "required_drawdown_windows": drawdown.get("required_windows"),
        "source_snapshot_path": str(risk_dir / "consensus_risk_source_snapshot.json"),
        "source_snapshot_checksum": source_snapshot_checksum,
        "policy_id": policy.get("policy_id"),
        "policy_version": policy.get("version"),
        "consensus_risk_manifest_path": str(risk_dir / "consensus_risk_manifest.json"),
        "consensus_exposure_samples_path": str(
            risk_dir / "consensus_exposure_samples.jsonl"
        ),
        "consensus_drawdown_pairs_path": str(risk_dir / "consensus_drawdown_pairs.jsonl"),
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


def _consensus_risk_live_source_validation(snapshot: Mapping[str, Any]) -> bool:
    daily_root = Path(_text(snapshot.get("daily_advisory_dir")))
    for bundle in _records(snapshot.get("daily_sources")):
        manifest = _mapping(_source_bundle_content(bundle, "daily_advisory_manifest.json"))
        if (
            validate_position_advisory_daily_artifact(
                daily_advisory_id=_text(manifest.get("daily_advisory_id")),
                output_dir=daily_root,
            ).get("status")
            != "PASS"
        ):
            return False
    replay_root = Path(_text(snapshot.get("historical_replay_dir")))
    for bundle in _records(snapshot.get("historical_replay_sources")):
        manifest = _mapping(_source_bundle_content(bundle, "historical_replay_manifest.json"))
        if (
            validate_historical_replay_artifact(
                replay_id=_text(manifest.get("replay_id")), output_dir=replay_root
            ).get("status")
            != "PASS"
        ):
            return False
    outcome_type = _text(snapshot.get("outcome_source_type"))
    root = Path(_text(snapshot.get("repair_dir" if outcome_type == "repair" else "backfill_dir")))
    manifest_name = (
        "backfill_repair_manifest.json" if outcome_type == "repair" else "backfill_manifest.json"
    )
    for bundle in _records(snapshot.get("outcome_sources")):
        manifest = _mapping(_source_bundle_content(bundle, manifest_name))
        if outcome_type == "repair":
            status = validate_backfill_repair_artifact(
                repair_id=_text(manifest.get("repair_id")), output_dir=root
            ).get("status")
        else:
            status = validate_backfill_outcome_artifact(
                backfill_id=_text(manifest.get("backfill_id")), output_dir=root
            ).get("status")
        if status != "PASS":
            return False
    return True


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


def _cached_data_quality_result(
    *, as_of: date, prices_path: Path, rates_path: Path, enforce: bool
) -> Any | None:
    if not enforce:
        return None
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
    if not quality.passed:
        raise DynamicV3OutcomeAccumulationError(
            f"cached data quality gate failed: {quality.status}"
        )
    return quality


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


def _aware_utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise DynamicV3OutcomeAccumulationError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


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


def _file_sha256(path: Path) -> str:
    if not path.is_file():
        return ""
    return sha256(path.read_bytes()).hexdigest()


def _immutable_source_bundle(root: Path) -> dict[str, Any]:
    files: dict[str, Any] = {}
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix()
        if path.suffix == ".json":
            content: Any = _read_json(path)
        elif path.suffix == ".jsonl":
            content = _read_jsonl(path)
        else:
            content = path.read_text(encoding="utf-8")
        files[relative] = {
            "path": str(path),
            "size_bytes": path.stat().st_size,
            "sha256": _file_sha256(path),
            "content": content,
        }
    return {"root": str(root), "files": files}


def _source_bundle_content(bundle: Mapping[str, Any], relative_path: str) -> Any:
    return _mapping(_mapping(bundle.get("files")).get(relative_path)).get("content")


def _source_bundle_matches(bundle: Mapping[str, Any]) -> bool:
    files = _mapping(bundle.get("files"))
    return bool(files) and all(
        (path := Path(_text(_mapping(row).get("path")))).is_file()
        and _mapping(row).get("sha256") == _file_sha256(path)
        and _mapping(row).get("size_bytes") == path.stat().st_size
        for row in files.values()
    )


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
