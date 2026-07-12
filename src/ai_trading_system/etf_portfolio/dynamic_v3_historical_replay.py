from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from hashlib import sha256
from itertools import combinations
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
DEFAULT_REPLAY_DIAGNOSIS_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "replay_diagnosis"
DEFAULT_BACKFILL_REPAIR_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "backfill_repair"
DEFAULT_VARIANT_COMPARISON_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "variant_comparison"
DEFAULT_RULE_CALIBRATION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "rule_calibration"
DEFAULT_REPLAY_FORWARD_BRIDGE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "replay_forward_bridge"
DEFAULT_PAPER_PORTFOLIO_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_portfolio"
REPLAY_INVENTORY_SNAPSHOT_SCHEMA_VERSION = "replay_inventory_source_snapshot.v2"

OUTCOME_MODE_HISTORICAL_REPLAY = "HISTORICAL_REPLAY"
PIT_SAFE_STATUSES = {"PIT_SAFE", "PIT_WARNING", "PIT_UNSAFE"}
HARD_PIT_LIMITATIONS = {
    "MISSING_TARGET_WEIGHTS",
    "MISSING_PRICE_DATA",
    "ADVISORY_GENERATED_AFTER_AS_OF_DATE",
}
WARNING_PIT_LIMITATIONS = {
    "OWNER_DECISION_MISSING",
    "PORTFOLIO_SNAPSHOT_APPROXIMATED_FROM_BASELINE",
    "PORTFOLIO_SNAPSHOT_APPROXIMATED_FROM_PAPER_STATE",
    "CONSENSUS_WEIGHTS_RECONSTRUCTED_FROM_TARGETS",
}
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
PENDING_REASON_ACTIONS = {
    "future_window_not_reached": "wait_or_use_older_replay_events",
    "missing_price_data": "repair_price_cache_or_keep_insufficient_data",
    "missing_target_weights": "extend_pit_safe_source_artifacts",
    "pit_unsafe": "exclude_from_replay_or_collect_pit_safe_source",
    "insufficient_replay_events": "extend_replay_inventory",
    "no_available_outcome_windows": "wait_or_replay_older_events",
    "paper_sim_insufficient_data": "review_replay_event_coverage",
    "review_waiting_for_backfill": "complete_backfill_before_review",
    "unknown": "manual_investigation_required",
}
# Pilot baseline from docs/requirements/TRADING-146_to_150_*.md; it only sizes
# forward confirmation watchlists and does not authorize policy or broker action.
FORWARD_CONFIRMATION_REQUIRED_EVENTS = 10


class DynamicV3HistoricalReplayError(ValueError):
    """Raised when historical replay artifacts fail closed."""


def _require_aware_utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise DynamicV3HistoricalReplayError(f"{field} must be timezone-aware")
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


def _selected_replay_advisories(
    *,
    start: date,
    end: date,
    generated_cutoff: datetime,
    daily_advisory_dir: Path,
) -> tuple[list[tuple[Path, dict[str, Any], date]], int]:
    selected: list[tuple[Path, dict[str, Any], date]] = []
    future_generated_excluded_count = 0
    for manifest_path in sorted(daily_advisory_dir.glob("*/daily_advisory_manifest.json")):
        manifest = _read_optional_json(manifest_path)
        if manifest is None:
            raise DynamicV3HistoricalReplayError(
                f"daily advisory manifest is unreadable: {manifest_path}"
            )
        as_of = _date_from_any(manifest.get("as_of"))
        if as_of is None:
            raise DynamicV3HistoricalReplayError(
                f"daily advisory as_of is invalid: {manifest_path}"
            )
        if not (start <= as_of <= end):
            continue
        source_generated_at = _datetime_from_any(manifest.get("generated_at"))
        if source_generated_at is None:
            raise DynamicV3HistoricalReplayError(
                f"daily advisory generated_at is invalid: {manifest_path}"
            )
        if source_generated_at > generated_cutoff:
            future_generated_excluded_count += 1
            continue
        if as_of > generated_cutoff.date():
            raise DynamicV3HistoricalReplayError(
                f"daily advisory as_of exceeds generated cutoff: {manifest_path}"
            )
        selected.append((manifest_path.parent, dict(manifest), as_of))
    daily_ids = [_text(manifest.get("daily_advisory_id")) for _, manifest, _ in selected]
    if any(not daily_id for daily_id in daily_ids) or len(daily_ids) != len(set(daily_ids)):
        raise DynamicV3HistoricalReplayError(
            "replay inventory requires unique non-empty daily advisory ids"
        )
    as_of_values = [as_of for _, _, as_of in selected]
    if len(as_of_values) != len(set(as_of_values)):
        raise DynamicV3HistoricalReplayError(
            "replay inventory requires at most one daily advisory per as_of"
        )
    return selected, future_generated_excluded_count


def _replay_inventory_source_snapshot(
    *,
    start: date,
    end: date,
    generated_cutoff: datetime,
    selected_advisories: Sequence[tuple[Path, Mapping[str, Any], date]],
    rows: Sequence[Mapping[str, Any]],
    future_generated_source_excluded_count: int,
    config_path: Path,
    prices_path: Path,
    owner_review_dir: Path,
    paper_portfolio_dir: Path,
    shadow_monitor_run_dir: Path,
    consensus_drift_dir: Path,
) -> dict[str, Any]:
    source_files: set[Path] = {config_path, prices_path}
    for advisory_dir, manifest, _ in selected_advisories:
        source_files.update(path for path in advisory_dir.rglob("*") if path.is_file())
        monitor_id = _text(manifest.get("source_shadow_monitor_run_id"))
        monitor_dir = shadow_monitor_run_dir / monitor_id
        if monitor_id and monitor_dir.is_dir():
            source_files.update(path for path in monitor_dir.rglob("*") if path.is_file())
    for root in (owner_review_dir, paper_portfolio_dir, consensus_drift_dir):
        if root.is_dir():
            source_files.update(path for path in root.rglob("*") if path.is_file())
    return {
        "schema_version": REPLAY_INVENTORY_SNAPSHOT_SCHEMA_VERSION,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "generated_cutoff": generated_cutoff.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "price_data_role": "outcome_availability_only_not_decision_input",
        "future_generated_source_excluded_count": future_generated_source_excluded_count,
        "selected_daily_advisory_ids": [
            _text(manifest.get("daily_advisory_id"))
            for _, manifest, _ in selected_advisories
        ],
        "selected_daily_sources": [
            {
                "advisory_dir": str(advisory_dir),
                "manifest_path": str(advisory_dir / "daily_advisory_manifest.json"),
                "daily_advisory_id": _text(manifest.get("daily_advisory_id")),
                "as_of": as_of.isoformat(),
            }
            for advisory_dir, manifest, as_of in selected_advisories
        ],
        "source_roots": {
            "config_path": str(config_path),
            "prices_path": str(prices_path),
            "owner_review_dir": str(owner_review_dir),
            "paper_portfolio_dir": str(paper_portfolio_dir),
            "shadow_monitor_run_dir": str(shadow_monitor_run_dir),
            "consensus_drift_dir": str(consensus_drift_dir),
        },
        "source_files": _replay_source_file_inventory(source_files),
        "cutoff_visible_bindings": [
            {
                "daily_advisory_id": row.get("daily_advisory_id"),
                "as_of": row.get("as_of"),
                "source_artifacts": _mapping(row.get("source_artifacts")),
                "available_inputs": _mapping(row.get("available_inputs")),
                "decision_inputs": _mapping(row.get("decision_inputs")),
            }
            for row in rows
        ],
        "canonical_rows": [dict(row) for row in rows],
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
    }


def _replay_source_file_inventory(paths: Sequence[Path] | set[Path]) -> list[dict[str, Any]]:
    inventory = []
    for path in sorted({item.resolve() for item in paths if item.is_file()}, key=str):
        inventory.append(
            {
                "path": str(path),
                "size_bytes": path.stat().st_size,
                "sha256": _sha256_file(path),
            }
        )
    return inventory


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _replay_inventory_rows_from_snapshot(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    roots = _mapping(snapshot.get("source_roots"))
    config = load_paper_portfolio_config(Path(_text(roots.get("config_path"))))
    owner_review_dir = Path(_text(roots.get("owner_review_dir")))
    paper_portfolio_dir = Path(_text(roots.get("paper_portfolio_dir")))
    shadow_monitor_run_dir = Path(_text(roots.get("shadow_monitor_run_dir")))
    consensus_drift_dir = Path(_text(roots.get("consensus_drift_dir")))
    price_index = _price_availability_index(Path(_text(roots.get("prices_path"))))
    generated_cutoff = _datetime_from_any(snapshot.get("generated_cutoff"))
    if generated_cutoff is None:
        raise DynamicV3HistoricalReplayError("snapshot generated cutoff is invalid")
    owner_reviews = _owner_reviews_at_cutoff(
        owner_review_dir / "owner_review_journal.jsonl",
        generated_cutoff=generated_cutoff,
    )
    rows = []
    for source in _records(snapshot.get("selected_daily_sources")):
        advisory_dir = Path(_text(source.get("advisory_dir")))
        manifest = _read_json(Path(_text(source.get("manifest_path"))))
        as_of = _date_from_any(source.get("as_of"))
        if (
            as_of is None
            or _text(manifest.get("daily_advisory_id"))
            != _text(source.get("daily_advisory_id"))
        ):
            raise DynamicV3HistoricalReplayError("snapshot daily advisory binding is invalid")
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
                generated_cutoff=generated_cutoff,
            )
        )
    return sorted(rows, key=lambda row: (row.get("as_of", ""), row.get("daily_advisory_id", "")))


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
    if start > end:
        raise DynamicV3HistoricalReplayError("replay inventory start must be on or before end")
    generated = _require_aware_utc(generated_at or datetime.now(UTC), "generated_at")
    selected_advisories, future_generated_excluded_count = _selected_replay_advisories(
        start=start,
        end=end,
        generated_cutoff=generated,
        daily_advisory_dir=daily_advisory_dir,
    )
    config = load_paper_portfolio_config(config_path)
    owner_reviews = _owner_reviews_at_cutoff(
        owner_review_dir / "owner_review_journal.jsonl",
        generated_cutoff=generated,
    )
    price_index = _price_availability_index(prices_path)
    rows = []
    for advisory_dir, manifest, as_of in selected_advisories:
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
                generated_cutoff=generated,
            )
        )
    rows = sorted(rows, key=lambda row: (row.get("as_of", ""), row.get("daily_advisory_id", "")))
    inventory_id = _stable_id("replay-inventory", start.isoformat(), end.isoformat(), generated)
    inventory_dir = _unique_dir(output_dir / inventory_id)
    inventory_dir.mkdir(parents=True, exist_ok=False)
    audit = _pit_safety_audit(rows)
    coverage = _replay_coverage_summary(rows, start=start, end=end)
    source_snapshot = _replay_inventory_source_snapshot(
        start=start,
        end=end,
        generated_cutoff=generated,
        selected_advisories=selected_advisories,
        rows=rows,
        future_generated_source_excluded_count=future_generated_excluded_count,
        config_path=config_path,
        prices_path=prices_path,
        owner_review_dir=owner_review_dir,
        paper_portfolio_dir=paper_portfolio_dir,
        shadow_monitor_run_dir=shadow_monitor_run_dir,
        consensus_drift_dir=consensus_drift_dir,
    )
    source_snapshot_path = inventory_dir / "replay_inventory_source_snapshot.json"
    _write_json(source_snapshot_path, source_snapshot)
    source_snapshot_checksum = _sha256_file(source_snapshot_path)
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
        "actual_start": _text(rows[0].get("as_of")) if rows else "",
        "actual_end": _text(rows[-1].get("as_of")) if rows else "",
        "evidence_cutoff": generated.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "future_generated_source_excluded_count": future_generated_excluded_count,
        "total_replay_events": len(rows),
        "pit_safe_count": audit["pit_safe_count"],
        "pit_warning_count": audit["pit_warning_count"],
        "pit_unsafe_count": audit["pit_unsafe_count"],
        "eligible_count": coverage["eligible_count"],
        "partial_count": coverage["partial_count"],
        "ineligible_count": coverage["ineligible_count"],
        "config_path": str(config_path),
        "prices_path": str(prices_path),
        "replay_artifact_inventory_path": str(inventory_dir / "replay_artifact_inventory.jsonl"),
        "pit_safety_audit_path": str(inventory_dir / "pit_safety_audit.json"),
        "replay_coverage_summary_path": str(inventory_dir / "replay_coverage_summary.json"),
        "replay_inventory_report_path": str(inventory_dir / "replay_inventory_report.md"),
        "source_snapshot_path": str(source_snapshot_path),
        "source_snapshot_checksum": source_snapshot_checksum,
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "price_data_role": "outcome_availability_only_not_decision_input",
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
        "source_snapshot": source_snapshot,
        "future_generated_source_excluded_count": future_generated_excluded_count,
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
        "replay_artifact_inventory": _read_jsonl(inventory_dir / "replay_artifact_inventory.jsonl"),
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
    snapshot_path = inventory_dir / "replay_inventory_source_snapshot.json"
    if not snapshot_path.is_file():
        legacy_checks = [
            _check(
                "manifest_exists",
                (inventory_dir / "replay_inventory_manifest.json").is_file(),
                inventory_id,
            ),
            _check(
                "inventory_rows_exist",
                (inventory_dir / "replay_artifact_inventory.jsonl").is_file(),
                inventory_id,
            ),
            _check(
                "pit_safety_audit_exists",
                (inventory_dir / "pit_safety_audit.json").is_file(),
                "legacy",
            ),
            _check(
                "coverage_summary_exists",
                (inventory_dir / "replay_coverage_summary.json").is_file(),
                "legacy",
            ),
            _check(
                "report_exists",
                (inventory_dir / "replay_inventory_report.md").is_file(),
                "legacy",
            ),
            _check(
                "inventory_id_matches",
                manifest.get("inventory_id") == inventory_id,
                inventory_id,
            ),
            _check(
                "pit_safety_status_valid",
                all(row.get("pit_safety_status") in PIT_SAFE_STATUSES for row in rows),
                "legacy shallow validation",
            ),
            _check(
                "broker_action_forbidden",
                manifest.get("broker_action_allowed") is False
                and manifest.get("broker_action_taken") is False,
                "legacy shallow validation",
            ),
        ]
        payload = _validation_payload(
            report_type="etf_dynamic_v3_replay_inventory_validation",
            artifact_id_key="inventory_id",
            artifact_id=inventory_id,
            checks=legacy_checks,
        )
        if payload["status"] == "PASS":
            payload["status"] = "PASS_WITH_WARNINGS"
        payload["source_snapshot_status"] = "LEGACY_UNSNAPSHOTTED"
        return payload
    snapshot = _read_optional_json(snapshot_path) or {}
    audit = _read_optional_json(inventory_dir / "pit_safety_audit.json") or {}
    coverage = _read_optional_json(inventory_dir / "replay_coverage_summary.json") or {}
    canonical_rows = _records(snapshot.get("canonical_rows"))
    try:
        expected_rows = _replay_inventory_rows_from_snapshot(snapshot)
        snapshot_replay_error = ""
    except Exception as exc:  # noqa: BLE001
        expected_rows = []
        snapshot_replay_error = str(exc)
    start = _date_from_any(snapshot.get("start"))
    end = _date_from_any(snapshot.get("end"))
    expected_audit = _pit_safety_audit(expected_rows)
    expected_coverage = (
        _replay_coverage_summary(expected_rows, start=start, end=end)
        if start is not None and end is not None
        else {}
    )
    source_files_valid = True
    for source in _records(snapshot.get("source_files")):
        path = Path(_text(source.get("path")))
        if (
            not path.is_file()
            or path.stat().st_size != _int(source.get("size_bytes"))
            or _sha256_file(path) != _text(source.get("sha256"))
        ):
            source_files_valid = False
            break
    expected_report = render_replay_inventory_report(
        manifest,
        expected_audit,
        expected_coverage,
        expected_rows,
    )
    report_path = inventory_dir / "replay_inventory_report.md"
    report_matches = report_path.is_file() and report_path.read_text(
        encoding="utf-8"
    ) == expected_report
    derived_manifest_matches = bool(start is not None and end is not None) and all(
        (
            manifest.get("start") == snapshot.get("start"),
            manifest.get("end") == snapshot.get("end"),
            manifest.get("evidence_cutoff") == snapshot.get("generated_cutoff"),
            manifest.get("market_regime") == snapshot.get("market_regime"),
            manifest.get("price_data_role") == snapshot.get("price_data_role"),
            manifest.get("future_generated_source_excluded_count")
            == snapshot.get("future_generated_source_excluded_count"),
            _int(manifest.get("total_replay_events")) == len(expected_rows),
            _int(manifest.get("pit_safe_count")) == _int(expected_audit.get("pit_safe_count")),
            _int(manifest.get("pit_warning_count"))
            == _int(expected_audit.get("pit_warning_count")),
            _int(manifest.get("pit_unsafe_count"))
            == _int(expected_audit.get("pit_unsafe_count")),
            _int(manifest.get("eligible_count"))
            == _int(expected_coverage.get("eligible_count")),
            _int(manifest.get("partial_count")) == _int(expected_coverage.get("partial_count")),
            _int(manifest.get("ineligible_count"))
            == _int(expected_coverage.get("ineligible_count")),
        )
    )
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
        _check("source_snapshot_exists", snapshot_path.exists(), inventory_id),
        _check(
            "source_snapshot_schema_valid",
            snapshot.get("schema_version") == REPLAY_INVENTORY_SNAPSHOT_SCHEMA_VERSION,
            REPLAY_INVENTORY_SNAPSHOT_SCHEMA_VERSION,
        ),
        _check(
            "source_snapshot_checksum_matches",
            _text(manifest.get("source_snapshot_checksum")) == _sha256_file(snapshot_path),
            "source snapshot checksum",
        ),
        _check("source_files_unchanged", source_files_valid, "source inventory"),
        _check(
            "snapshot_rows_recomputed",
            not snapshot_replay_error and canonical_rows == expected_rows,
            snapshot_replay_error or "snapshot rows",
        ),
        _check("inventory_rows_match_snapshot", rows == expected_rows, "canonical rows"),
        _check("pit_audit_recomputed", audit == expected_audit, "PIT audit"),
        _check("coverage_recomputed", coverage == expected_coverage, "coverage"),
        _check("manifest_derived_fields_match", derived_manifest_matches, "manifest"),
        _check("report_recomputed", report_matches, "Markdown report"),
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
            "hard_limitations_are_pit_unsafe",
            all(
                not (HARD_PIT_LIMITATIONS & set(_texts(row.get("replay_limitations"))))
                or (
                    row.get("pit_safety_status") == "PIT_UNSAFE"
                    and row.get("replay_eligibility") == "INELIGIBLE"
                )
                for row in rows
            ),
            "hard PIT limitations must be PIT_UNSAFE / INELIGIBLE",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action forbidden",
        ),
        _check(
            "price_role_is_outcome_only",
            manifest.get("price_data_role")
            == "outcome_availability_only_not_decision_input",
            "future prices are not decision inputs",
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
        "variant_performance_summary_path": str(backfill_dir / "variant_performance_summary.json"),
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
        "historical_paper_sim_manifest_path": str(sim_dir / "historical_paper_sim_manifest.json"),
        "simulated_paper_state_history_path": str(sim_dir / "simulated_paper_state_history.jsonl"),
        "simulated_trade_ledger_path": str(sim_dir / "simulated_trade_ledger.jsonl"),
        "simulated_performance_summary_path": str(sim_dir / "simulated_performance_summary.json"),
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
        "simulated_performance_summary": _read_json(sim_dir / "simulated_performance_summary.json"),
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
        _check(
            "state_history_present_or_insufficient_data",
            bool(history) or manifest.get("status") == "INSUFFICIENT_DATA",
            "state history or explicit insufficient data",
        ),
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
        "advisory_rule_effectiveness_path": str(review_dir / "advisory_rule_effectiveness.json"),
        "calibration_recommendations_path": str(review_dir / "calibration_recommendations.json"),
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
        "advisory_rule_effectiveness": _read_json(review_dir / "advisory_rule_effectiveness.json"),
        "calibration_recommendations": _read_json(review_dir / "calibration_recommendations.json"),
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


def run_replay_diagnosis(
    *,
    inventory_id: str,
    replay_id: str,
    backfill_id: str,
    sim_id: str,
    review_id: str,
    inventory_dir: Path = DEFAULT_REPLAY_INVENTORY_DIR,
    replay_dir: Path = DEFAULT_HISTORICAL_REPLAY_DIR,
    backfill_dir: Path = DEFAULT_BACKFILLED_OUTCOME_DIR,
    sim_dir: Path = DEFAULT_HISTORICAL_PAPER_SIM_DIR,
    review_dir: Path = DEFAULT_REPLAY_PERFORMANCE_REVIEW_DIR,
    output_dir: Path = DEFAULT_REPLAY_DIAGNOSIS_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_inventory_dir = inventory_dir / inventory_id
    source_replay_dir = replay_dir / replay_id
    source_backfill_dir = backfill_dir / backfill_id
    source_sim_dir = sim_dir / sim_id
    source_review_dir = review_dir / review_id

    inventory_manifest = _read_json(source_inventory_dir / "replay_inventory_manifest.json")
    inventory_rows = _read_jsonl(source_inventory_dir / "replay_artifact_inventory.jsonl")
    inventory_coverage = (
        _read_optional_json(source_inventory_dir / "replay_coverage_summary.json") or {}
    )
    replay_manifest = _read_json(source_replay_dir / "historical_replay_manifest.json")
    replay_events = _read_jsonl(source_replay_dir / "replay_events.jsonl")
    replay_summary = _read_optional_json(source_replay_dir / "replay_action_summary.json") or {}
    backfill_manifest = _read_json(source_backfill_dir / "backfill_manifest.json")
    outcome_rows = _read_jsonl(source_backfill_dir / "replay_outcome_windows.jsonl")
    sim_manifest = _read_json(source_sim_dir / "historical_paper_sim_manifest.json")
    sim_state_history = _read_jsonl(source_sim_dir / "simulated_paper_state_history.jsonl")
    sim_summary = _read_optional_json(source_sim_dir / "simulated_performance_summary.json") or {}
    review_manifest = _read_json(source_review_dir / "replay_performance_manifest.json")
    calibration = _read_optional_json(source_review_dir / "calibration_recommendations.json") or {}

    pending_reasons = _replay_pending_reason_summary(
        inventory_rows=inventory_rows,
        replay_summary=replay_summary,
        backfill_manifest=backfill_manifest,
        outcome_rows=outcome_rows,
        sim_summary=sim_summary,
        review_manifest=review_manifest,
    )
    blocking_reasons = [
        row["reason"] for row in pending_reasons["pending_reasons"] if row.get("blocking")
    ]
    coverage = _replay_diagnosis_coverage_breakdown(
        inventory_manifest=inventory_manifest,
        inventory_coverage=inventory_coverage,
        replay_manifest=replay_manifest,
        replay_summary=replay_summary,
        backfill_manifest=backfill_manifest,
        sim_manifest=sim_manifest,
        sim_summary=sim_summary,
        sim_event_count=len(sim_state_history),
        review_manifest=review_manifest,
        calibration=calibration,
    )
    artifact_health = [
        _artifact_health_row(
            "replay_inventory",
            inventory_id,
            source_inventory_dir / "replay_inventory_manifest.json",
            inventory_manifest,
            len(inventory_rows),
        ),
        _artifact_health_row(
            "historical_replay",
            replay_id,
            source_replay_dir / "historical_replay_manifest.json",
            replay_manifest,
            len(replay_events),
        ),
        _artifact_health_row(
            "backfilled_outcome",
            backfill_id,
            source_backfill_dir / "backfill_manifest.json",
            backfill_manifest,
            len(outcome_rows),
        ),
        _artifact_health_row(
            "historical_paper_sim",
            sim_id,
            source_sim_dir / "historical_paper_sim_manifest.json",
            sim_manifest,
            len(sim_state_history),
        ),
        _artifact_health_row(
            "replay_performance_review",
            review_id,
            source_review_dir / "replay_performance_manifest.json",
            review_manifest,
            len(_records(calibration.get("recommendations"))),
        ),
    ]
    status = "PASS" if not blocking_reasons else "PASS_WITH_WARNINGS"
    if not inventory_rows and not replay_events and not outcome_rows:
        status = "INSUFFICIENT_DATA"
    diagnosis_id = _stable_id(
        "replay-diagnosis",
        inventory_id,
        replay_id,
        backfill_id,
        sim_id,
        review_id,
        generated.isoformat(),
    )
    diagnosis_dir = _unique_dir(output_dir / diagnosis_id)
    diagnosis_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_replay_diagnosis_manifest",
        "diagnosis_id": diagnosis_dir.name,
        "generated_at": generated.isoformat(),
        "status": status,
        "inventory_id": inventory_id,
        "replay_id": replay_id,
        "backfill_id": backfill_id,
        "sim_id": sim_id,
        "review_id": review_id,
        "blocking_pending_reasons": blocking_reasons,
        "can_enter_variant_comparison": coverage["backfill"]["available_windows"] > 0,
        "source_inventory_path": str(source_inventory_dir / "replay_inventory_manifest.json"),
        "source_replay_path": str(source_replay_dir / "historical_replay_manifest.json"),
        "source_backfill_path": str(source_backfill_dir / "backfill_manifest.json"),
        "source_sim_path": str(source_sim_dir / "historical_paper_sim_manifest.json"),
        "source_review_path": str(source_review_dir / "replay_performance_manifest.json"),
        "replay_diagnosis_manifest_path": str(diagnosis_dir / "replay_diagnosis_manifest.json"),
        "replay_coverage_breakdown_path": str(diagnosis_dir / "replay_coverage_breakdown.json"),
        "replay_pending_reason_summary_path": str(
            diagnosis_dir / "replay_pending_reason_summary.json"
        ),
        "replay_artifact_health_matrix_path": str(
            diagnosis_dir / "replay_artifact_health_matrix.jsonl"
        ),
        "replay_diagnosis_report_path": str(diagnosis_dir / "replay_diagnosis_report.md"),
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
    _write_json(diagnosis_dir / "replay_diagnosis_manifest.json", manifest)
    _write_json(diagnosis_dir / "replay_coverage_breakdown.json", coverage)
    _write_json(diagnosis_dir / "replay_pending_reason_summary.json", pending_reasons)
    _write_jsonl(diagnosis_dir / "replay_artifact_health_matrix.jsonl", artifact_health)
    _write_text(
        diagnosis_dir / "replay_diagnosis_report.md",
        render_replay_diagnosis_report(manifest, coverage, pending_reasons, artifact_health),
    )
    _update_latest_pointer(
        "latest_replay_diagnosis",
        diagnosis_dir.name,
        diagnosis_dir / "replay_diagnosis_manifest.json",
    )
    return {
        "diagnosis_id": diagnosis_dir.name,
        "diagnosis_dir": diagnosis_dir,
        "manifest": manifest,
        "coverage_breakdown": coverage,
        "pending_reason_summary": pending_reasons,
        "artifact_health_matrix": artifact_health,
    }


def replay_diagnosis_report_payload(
    *,
    diagnosis_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_REPLAY_DIAGNOSIS_DIR,
) -> dict[str, Any]:
    diagnosis_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=diagnosis_id if not latest else None,
        pointer_name="latest_replay_diagnosis",
    )
    return {
        **_read_json(diagnosis_dir / "replay_diagnosis_manifest.json"),
        "replay_coverage_breakdown": _read_json(diagnosis_dir / "replay_coverage_breakdown.json"),
        "replay_pending_reason_summary": _read_json(
            diagnosis_dir / "replay_pending_reason_summary.json"
        ),
        "replay_artifact_health_matrix": _read_jsonl(
            diagnosis_dir / "replay_artifact_health_matrix.jsonl"
        ),
        "diagnosis_dir": str(diagnosis_dir),
    }


def validate_replay_diagnosis_artifact(
    *,
    diagnosis_id: str,
    output_dir: Path = DEFAULT_REPLAY_DIAGNOSIS_DIR,
) -> dict[str, Any]:
    diagnosis_dir = output_dir / diagnosis_id
    manifest = _read_optional_json(diagnosis_dir / "replay_diagnosis_manifest.json") or {}
    reasons = _read_optional_json(diagnosis_dir / "replay_pending_reason_summary.json") or {}
    health = _read_jsonl(diagnosis_dir / "replay_artifact_health_matrix.jsonl")
    valid_reasons = set(PENDING_REASON_ACTIONS)
    checks = [
        _check("manifest_exists", (diagnosis_dir / "replay_diagnosis_manifest.json").exists(), ""),
        _check(
            "coverage_breakdown_exists",
            (diagnosis_dir / "replay_coverage_breakdown.json").exists(),
            "",
        ),
        _check(
            "pending_reason_summary_exists",
            (diagnosis_dir / "replay_pending_reason_summary.json").exists(),
            "",
        ),
        _check(
            "artifact_health_matrix_exists",
            (diagnosis_dir / "replay_artifact_health_matrix.jsonl").exists(),
            "",
        ),
        _check("report_exists", (diagnosis_dir / "replay_diagnosis_report.md").exists(), ""),
        _check("diagnosis_id_matches", manifest.get("diagnosis_id") == diagnosis_id, diagnosis_id),
        _check(
            "pending_reasons_known",
            all(
                row.get("reason") in valid_reasons
                for row in _records(reasons.get("pending_reasons"))
            ),
            "known pending reasons",
        ),
        _check(
            "artifact_health_has_sources",
            all(row.get("exists") is True and row.get("artifact_id") for row in health),
            "source artifacts",
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
        report_type="etf_dynamic_v3_replay_diagnosis_validation",
        artifact_id_key="diagnosis_id",
        artifact_id=diagnosis_id,
        checks=checks,
    )


def run_backfill_repair(
    *,
    backfill_id: str,
    diagnosis_id: str,
    backfill_dir: Path = DEFAULT_BACKFILLED_OUTCOME_DIR,
    diagnosis_dir: Path = DEFAULT_REPLAY_DIAGNOSIS_DIR,
    replay_dir: Path = DEFAULT_HISTORICAL_REPLAY_DIR,
    output_dir: Path = DEFAULT_BACKFILL_REPAIR_DIR,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    enforce_data_quality_gate: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_backfill_dir = backfill_dir / backfill_id
    source_diagnosis_dir = diagnosis_dir / diagnosis_id
    backfill_manifest = _read_json(source_backfill_dir / "backfill_manifest.json")
    diagnosis_manifest = _read_json(source_diagnosis_dir / "replay_diagnosis_manifest.json")
    original_rows = _read_jsonl(source_backfill_dir / "replay_outcome_windows.jsonl")
    replay_id = _text(backfill_manifest.get("replay_id"))
    source_replay_dir = replay_dir / replay_id
    replay_events = _read_jsonl(source_replay_dir / "replay_events.jsonl")

    repair_id = _stable_id("backfill-repair", backfill_id, diagnosis_id, generated.isoformat())
    repair_dir = _unique_dir(output_dir / repair_id)
    repair_dir.mkdir(parents=True, exist_ok=False)
    quality_status = "SKIPPED_EXPLICIT_TEST_FIXTURE"
    quality_report_path = ""
    if enforce_data_quality_gate:
        quality_report = repair_dir / "validate_data_quality_report.md"
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
                f"backfill repair data quality gate failed: {quality.status}"
            )
    prices = _load_prices_for_replay(prices_path, replay_events)
    price_dates = _available_price_dates(prices)
    event_map = {_text(event.get("replay_event_id")): event for event in replay_events}
    repaired_rows, actions = _repair_outcome_rows(
        original_rows=original_rows,
        event_map=event_map,
        prices=prices,
        price_dates=price_dates,
        generated_date=generated.date(),
    )
    before = _availability_counts(original_rows)
    after = _availability_counts(repaired_rows)
    repaired_count = sum(
        1
        for action in actions
        if action.get("original_status") != action.get("new_status")
        and action.get("new_status") == "AVAILABLE"
    )
    delta = {
        "before": before,
        "after": after,
        "repaired_count": repaired_count,
        "still_pending_count": after["pending"],
        "still_insufficient_count": after["insufficient_data"],
    }
    status = "PASS" if repaired_count else "PASS_WITH_WARNINGS"
    if not repaired_rows or after["available"] == 0:
        status = "INSUFFICIENT_DATA" if not after["pending"] else "PENDING"
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backfill_repair_manifest",
        "repair_id": repair_dir.name,
        "backfill_id": backfill_id,
        "diagnosis_id": diagnosis_id,
        "replay_id": replay_id,
        "generated_at": generated.isoformat(),
        "status": status,
        "data_quality_status": quality_status,
        "data_quality_report_path": quality_report_path,
        "future_data_used_in_decision": False,
        "source_backfill_path": str(source_backfill_dir / "backfill_manifest.json"),
        "source_diagnosis_path": str(source_diagnosis_dir / "replay_diagnosis_manifest.json"),
        "source_replay_path": str(source_replay_dir / "historical_replay_manifest.json"),
        "backfill_repair_manifest_path": str(repair_dir / "backfill_repair_manifest.json"),
        "repair_actions_path": str(repair_dir / "repair_actions.jsonl"),
        "repaired_outcome_windows_path": str(repair_dir / "repaired_outcome_windows.jsonl"),
        "backfill_availability_delta_path": str(repair_dir / "backfill_availability_delta.json"),
        "backfill_repair_report_path": str(repair_dir / "backfill_repair_report.md"),
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(repair_dir / "backfill_repair_manifest.json", manifest)
    _write_jsonl(repair_dir / "repair_actions.jsonl", actions)
    _write_jsonl(repair_dir / "repaired_outcome_windows.jsonl", repaired_rows)
    _write_json(repair_dir / "backfill_availability_delta.json", delta)
    _write_text(
        repair_dir / "backfill_repair_report.md",
        render_backfill_repair_report(manifest, delta, actions),
    )
    _update_latest_pointer(
        "latest_backfill_repair",
        repair_dir.name,
        repair_dir / "backfill_repair_manifest.json",
    )
    return {
        "repair_id": repair_dir.name,
        "repair_dir": repair_dir,
        "manifest": manifest,
        "repair_actions": actions,
        "repaired_outcome_windows": repaired_rows,
        "backfill_availability_delta": delta,
        "diagnosis_manifest": diagnosis_manifest,
    }


def backfill_repair_report_payload(
    *,
    repair_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_BACKFILL_REPAIR_DIR,
) -> dict[str, Any]:
    repair_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=repair_id if not latest else None,
        pointer_name="latest_backfill_repair",
    )
    return {
        **_read_json(repair_dir / "backfill_repair_manifest.json"),
        "repair_actions": _read_jsonl(repair_dir / "repair_actions.jsonl"),
        "repaired_outcome_windows": _read_jsonl(repair_dir / "repaired_outcome_windows.jsonl"),
        "backfill_availability_delta": _read_json(repair_dir / "backfill_availability_delta.json"),
        "repair_dir": str(repair_dir),
    }


def validate_backfill_repair_artifact(
    *,
    repair_id: str,
    output_dir: Path = DEFAULT_BACKFILL_REPAIR_DIR,
) -> dict[str, Any]:
    repair_dir = output_dir / repair_id
    manifest = _read_optional_json(repair_dir / "backfill_repair_manifest.json") or {}
    actions = _read_jsonl(repair_dir / "repair_actions.jsonl")
    rows = _read_jsonl(repair_dir / "repaired_outcome_windows.jsonl")
    checks = [
        _check("manifest_exists", (repair_dir / "backfill_repair_manifest.json").exists(), ""),
        _check("repair_actions_exists", (repair_dir / "repair_actions.jsonl").exists(), ""),
        _check(
            "repaired_windows_exists",
            (repair_dir / "repaired_outcome_windows.jsonl").exists(),
            "",
        ),
        _check(
            "availability_delta_exists",
            (repair_dir / "backfill_availability_delta.json").exists(),
            "",
        ),
        _check("report_exists", (repair_dir / "backfill_repair_report.md").exists(), ""),
        _check("repair_id_matches", manifest.get("repair_id") == repair_id, repair_id),
        _check(
            "window_status_valid",
            all(row.get("outcome_status") in OUTCOME_STATUSES for row in rows),
            "outcome status",
        ),
        _check(
            "no_future_data_used_in_decision",
            manifest.get("future_data_used_in_decision") is False
            and all(action.get("future_data_used_in_decision") is False for action in actions),
            "future data must not enter replay decision input",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action forbidden",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_backfill_repair_validation",
        artifact_id_key="repair_id",
        artifact_id=repair_id,
        checks=checks,
    )


def run_variant_comparison(
    *,
    backfill_id: str,
    repair_id: str | None = None,
    backfill_dir: Path = DEFAULT_BACKFILLED_OUTCOME_DIR,
    repair_dir: Path = DEFAULT_BACKFILL_REPAIR_DIR,
    output_dir: Path = DEFAULT_VARIANT_COMPARISON_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_backfill_dir = backfill_dir / backfill_id
    backfill_manifest = _read_json(source_backfill_dir / "backfill_manifest.json")
    source_repair_dir = repair_dir / repair_id if repair_id else None
    repair_manifest = (
        _read_json(source_repair_dir / "backfill_repair_manifest.json")
        if source_repair_dir is not None
        else {}
    )
    rows = (
        _read_jsonl(source_repair_dir / "repaired_outcome_windows.jsonl")
        if source_repair_dir is not None
        else _read_jsonl(source_backfill_dir / "replay_outcome_windows.jsonl")
    )
    metrics = _variant_window_metrics(rows)
    pairwise = _variant_pairwise_comparison(rows)
    ranking = _variant_rank_summary(metrics)
    comparison_id = _stable_id(
        "variant-comparison",
        backfill_id,
        repair_id or "source-backfill",
        generated.isoformat(),
    )
    comparison_dir = _unique_dir(output_dir / comparison_id)
    comparison_dir.mkdir(parents=True, exist_ok=False)
    status = (
        "PASS"
        if ranking["recommendation_confidence"] != "INSUFFICIENT_DATA"
        else "INSUFFICIENT_DATA"
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_variant_comparison_manifest",
        "comparison_id": comparison_dir.name,
        "backfill_id": backfill_id,
        "repair_id": repair_id or "",
        "generated_at": generated.isoformat(),
        "status": status,
        "best_variant": ranking["best_variant"],
        "recommendation_confidence": ranking["recommendation_confidence"],
        "source_backfill_path": str(source_backfill_dir / "backfill_manifest.json"),
        "source_repair_path": (
            ""
            if source_repair_dir is None
            else str(source_repair_dir / "backfill_repair_manifest.json")
        ),
        "variant_comparison_manifest_path": str(
            comparison_dir / "variant_comparison_manifest.json"
        ),
        "variant_window_metrics_path": str(comparison_dir / "variant_window_metrics.jsonl"),
        "variant_pairwise_comparison_path": str(
            comparison_dir / "variant_pairwise_comparison.json"
        ),
        "variant_rank_summary_path": str(comparison_dir / "variant_rank_summary.json"),
        "variant_comparison_report_path": str(comparison_dir / "variant_comparison_report.md"),
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(comparison_dir / "variant_comparison_manifest.json", manifest)
    _write_jsonl(comparison_dir / "variant_window_metrics.jsonl", metrics)
    _write_json(comparison_dir / "variant_pairwise_comparison.json", pairwise)
    _write_json(comparison_dir / "variant_rank_summary.json", ranking)
    _write_text(
        comparison_dir / "variant_comparison_report.md",
        render_variant_comparison_report(manifest, metrics, pairwise, ranking),
    )
    _update_latest_pointer(
        "latest_variant_comparison",
        comparison_dir.name,
        comparison_dir / "variant_comparison_manifest.json",
    )
    return {
        "comparison_id": comparison_dir.name,
        "comparison_dir": comparison_dir,
        "manifest": manifest,
        "variant_window_metrics": metrics,
        "variant_pairwise_comparison": pairwise,
        "variant_rank_summary": ranking,
        "backfill_manifest": backfill_manifest,
        "repair_manifest": repair_manifest,
    }


def variant_comparison_report_payload(
    *,
    comparison_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_VARIANT_COMPARISON_DIR,
) -> dict[str, Any]:
    comparison_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=comparison_id if not latest else None,
        pointer_name="latest_variant_comparison",
    )
    return {
        **_read_json(comparison_dir / "variant_comparison_manifest.json"),
        "variant_window_metrics": _read_jsonl(comparison_dir / "variant_window_metrics.jsonl"),
        "variant_pairwise_comparison": _read_json(
            comparison_dir / "variant_pairwise_comparison.json"
        ),
        "variant_rank_summary": _read_json(comparison_dir / "variant_rank_summary.json"),
        "comparison_dir": str(comparison_dir),
    }


def validate_variant_comparison_artifact(
    *,
    comparison_id: str,
    output_dir: Path = DEFAULT_VARIANT_COMPARISON_DIR,
) -> dict[str, Any]:
    comparison_dir = output_dir / comparison_id
    manifest = _read_optional_json(comparison_dir / "variant_comparison_manifest.json") or {}
    metrics = _read_jsonl(comparison_dir / "variant_window_metrics.jsonl")
    ranking = _read_optional_json(comparison_dir / "variant_rank_summary.json") or {}
    checks = [
        _check(
            "manifest_exists", (comparison_dir / "variant_comparison_manifest.json").exists(), ""
        ),
        _check("metrics_exists", (comparison_dir / "variant_window_metrics.jsonl").exists(), ""),
        _check(
            "pairwise_exists",
            (comparison_dir / "variant_pairwise_comparison.json").exists(),
            "",
        ),
        _check("ranking_exists", (comparison_dir / "variant_rank_summary.json").exists(), ""),
        _check("report_exists", (comparison_dir / "variant_comparison_report.md").exists(), ""),
        _check(
            "comparison_id_matches",
            manifest.get("comparison_id") == comparison_id,
            comparison_id,
        ),
        _check(
            "known_variants_present",
            {row.get("variant") for row in metrics}.issubset(set(REPLAY_VARIANTS)),
            "known replay variants",
        ),
        _check(
            "insufficient_data_marked",
            bool(ranking.get("ranking"))
            or ranking.get("recommendation_confidence") == "INSUFFICIENT_DATA",
            "empty ranking must be insufficient data",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action forbidden",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_variant_comparison_validation",
        artifact_id_key="comparison_id",
        artifact_id=comparison_id,
        checks=checks,
    )


def run_rule_calibration(
    *,
    comparison_id: str,
    comparison_dir: Path = DEFAULT_VARIANT_COMPARISON_DIR,
    output_dir: Path = DEFAULT_RULE_CALIBRATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_comparison_dir = comparison_dir / comparison_id
    comparison_manifest = _read_json(source_comparison_dir / "variant_comparison_manifest.json")
    ranking = _read_json(source_comparison_dir / "variant_rank_summary.json")
    metrics = _read_jsonl(source_comparison_dir / "variant_window_metrics.jsonl")
    diagnostics = _advisory_rule_diagnostics_from_comparison(comparison_manifest, ranking, metrics)
    proposals = _policy_adjustment_proposals_from_diagnostics(
        comparison_manifest,
        ranking,
        diagnostics,
    )
    safety = {
        "auto_apply": False,
        "production_effect": "none",
        "broker_action_allowed": False,
        "owner_approval_required": True,
        "sufficient_sample_size": ranking.get("recommendation_confidence") != "INSUFFICIENT_DATA",
        "requires_forward_confirmation": True,
    }
    calibration_id = _stable_id("rule-calibration", comparison_id, generated.isoformat())
    calibration_dir = _unique_dir(output_dir / calibration_id)
    calibration_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_rule_calibration_manifest",
        "calibration_id": calibration_dir.name,
        "comparison_id": comparison_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "proposal_count": len(proposals["proposals"]),
        "auto_apply": False,
        "owner_approval_required": True,
        "source_comparison_path": str(source_comparison_dir / "variant_comparison_manifest.json"),
        "rule_calibration_manifest_path": str(calibration_dir / "rule_calibration_manifest.json"),
        "advisory_rule_diagnostics_path": str(calibration_dir / "advisory_rule_diagnostics.json"),
        "proposed_policy_adjustments_path": str(
            calibration_dir / "proposed_policy_adjustments.json"
        ),
        "calibration_safety_checks_path": str(calibration_dir / "calibration_safety_checks.json"),
        "rule_calibration_report_path": str(calibration_dir / "rule_calibration_report.md"),
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
    _write_json(calibration_dir / "rule_calibration_manifest.json", manifest)
    _write_json(calibration_dir / "advisory_rule_diagnostics.json", diagnostics)
    _write_json(calibration_dir / "proposed_policy_adjustments.json", proposals)
    _write_json(calibration_dir / "calibration_safety_checks.json", safety)
    _write_text(
        calibration_dir / "rule_calibration_report.md",
        render_rule_calibration_report(manifest, diagnostics, proposals, safety),
    )
    _update_latest_pointer(
        "latest_rule_calibration",
        calibration_dir.name,
        calibration_dir / "rule_calibration_manifest.json",
    )
    return {
        "calibration_id": calibration_dir.name,
        "calibration_dir": calibration_dir,
        "manifest": manifest,
        "advisory_rule_diagnostics": diagnostics,
        "proposed_policy_adjustments": proposals,
        "calibration_safety_checks": safety,
    }


def rule_calibration_report_payload(
    *,
    calibration_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_RULE_CALIBRATION_DIR,
) -> dict[str, Any]:
    calibration_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=calibration_id if not latest else None,
        pointer_name="latest_rule_calibration",
    )
    return {
        **_read_json(calibration_dir / "rule_calibration_manifest.json"),
        "advisory_rule_diagnostics": _read_json(calibration_dir / "advisory_rule_diagnostics.json"),
        "proposed_policy_adjustments": _read_json(
            calibration_dir / "proposed_policy_adjustments.json"
        ),
        "calibration_safety_checks": _read_json(calibration_dir / "calibration_safety_checks.json"),
        "calibration_dir": str(calibration_dir),
    }


def validate_rule_calibration_artifact(
    *,
    calibration_id: str,
    output_dir: Path = DEFAULT_RULE_CALIBRATION_DIR,
) -> dict[str, Any]:
    calibration_dir = output_dir / calibration_id
    manifest = _read_optional_json(calibration_dir / "rule_calibration_manifest.json") or {}
    proposals = _read_optional_json(calibration_dir / "proposed_policy_adjustments.json") or {}
    safety = _read_optional_json(calibration_dir / "calibration_safety_checks.json") or {}
    checks = [
        _check(
            "manifest_exists", (calibration_dir / "rule_calibration_manifest.json").exists(), ""
        ),
        _check(
            "diagnostics_exists",
            (calibration_dir / "advisory_rule_diagnostics.json").exists(),
            "",
        ),
        _check(
            "proposals_exists",
            (calibration_dir / "proposed_policy_adjustments.json").exists(),
            "",
        ),
        _check(
            "safety_checks_exists",
            (calibration_dir / "calibration_safety_checks.json").exists(),
            "",
        ),
        _check("report_exists", (calibration_dir / "rule_calibration_report.md").exists(), ""),
        _check(
            "calibration_id_matches",
            manifest.get("calibration_id") == calibration_id,
            calibration_id,
        ),
        _check(
            "proposals_are_manual_only",
            all(
                row.get("auto_apply") is False and row.get("requires_owner_approval") is True
                for row in _records(proposals.get("proposals"))
            ),
            "manual-only proposals",
        ),
        _check(
            "safety_blocks_auto_apply",
            safety.get("auto_apply") is False
            and safety.get("broker_action_allowed") is False
            and safety.get("owner_approval_required") is True,
            "auto apply disabled",
        ),
        _check(
            "production_mutation_forbidden",
            manifest.get("production_candidate_generated") is False
            and manifest.get("automatic_candidate_promotion") is False
            and manifest.get("official_target_weights_mutated") is False
            and manifest.get("baseline_config_mutated") is False,
            "no production mutation",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_rule_calibration_validation",
        artifact_id_key="calibration_id",
        artifact_id=calibration_id,
        checks=checks,
    )


def run_replay_forward_bridge(
    *,
    diagnosis_id: str,
    comparison_id: str,
    calibration_id: str,
    diagnosis_dir: Path = DEFAULT_REPLAY_DIAGNOSIS_DIR,
    comparison_dir: Path = DEFAULT_VARIANT_COMPARISON_DIR,
    calibration_dir: Path = DEFAULT_RULE_CALIBRATION_DIR,
    output_dir: Path = DEFAULT_REPLAY_FORWARD_BRIDGE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_diagnosis_dir = diagnosis_dir / diagnosis_id
    source_comparison_dir = comparison_dir / comparison_id
    source_calibration_dir = calibration_dir / calibration_id
    diagnosis_manifest = _read_json(source_diagnosis_dir / "replay_diagnosis_manifest.json")
    pending_reasons = _read_json(source_diagnosis_dir / "replay_pending_reason_summary.json")
    comparison_manifest = _read_json(source_comparison_dir / "variant_comparison_manifest.json")
    ranking = _read_json(source_comparison_dir / "variant_rank_summary.json")
    calibration_manifest = _read_json(source_calibration_dir / "rule_calibration_manifest.json")
    proposals = _read_json(source_calibration_dir / "proposed_policy_adjustments.json")

    focus = _forward_tracking_focus_from_replay(
        diagnosis_manifest=diagnosis_manifest,
        pending_reasons=pending_reasons,
        comparison_manifest=comparison_manifest,
        ranking=ranking,
        proposals=proposals,
    )
    weekly_updates = _weekly_review_updates_from_focus(focus)
    bridge_id = _stable_id(
        "replay-forward-bridge",
        diagnosis_id,
        comparison_id,
        calibration_id,
        generated.isoformat(),
    )
    bridge_dir = _unique_dir(output_dir / bridge_id)
    bridge_dir.mkdir(parents=True, exist_ok=False)
    next_action = focus["next_actions"][0] if focus["next_actions"] else "continue_forward_tracking"
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_replay_forward_bridge_manifest",
        "bridge_id": bridge_dir.name,
        "diagnosis_id": diagnosis_id,
        "comparison_id": comparison_id,
        "calibration_id": calibration_id,
        "generated_at": generated.isoformat(),
        "status": focus["forward_tracking_status"],
        "historical_replay_status": diagnosis_manifest.get("status", "MISSING"),
        "best_variant": comparison_manifest.get("best_variant", "MISSING"),
        "calibration_confidence": comparison_manifest.get(
            "recommendation_confidence",
            "INSUFFICIENT_DATA",
        ),
        "next_action": next_action,
        "source_diagnosis_path": str(source_diagnosis_dir / "replay_diagnosis_manifest.json"),
        "source_comparison_path": str(source_comparison_dir / "variant_comparison_manifest.json"),
        "source_calibration_path": str(source_calibration_dir / "rule_calibration_manifest.json"),
        "bridge_manifest_path": str(bridge_dir / "bridge_manifest.json"),
        "forward_tracking_focus_path": str(bridge_dir / "forward_tracking_focus.json"),
        "weekly_review_updates_path": str(bridge_dir / "weekly_review_updates.json"),
        "replay_forward_bridge_report_path": str(bridge_dir / "replay_forward_bridge_report.md"),
        "reader_brief_section_path": str(bridge_dir / "reader_brief_section.md"),
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
    _write_json(bridge_dir / "bridge_manifest.json", manifest)
    _write_json(bridge_dir / "forward_tracking_focus.json", focus)
    _write_json(bridge_dir / "weekly_review_updates.json", weekly_updates)
    _write_text(
        bridge_dir / "replay_forward_bridge_report.md",
        render_replay_forward_bridge_report(manifest, focus, weekly_updates, proposals),
    )
    _write_text(
        bridge_dir / "reader_brief_section.md",
        render_replay_forward_bridge_reader_brief(manifest, focus),
    )
    _update_latest_pointer(
        "latest_replay_forward_bridge",
        bridge_dir.name,
        bridge_dir / "bridge_manifest.json",
    )
    return {
        "bridge_id": bridge_dir.name,
        "bridge_dir": bridge_dir,
        "manifest": manifest,
        "forward_tracking_focus": focus,
        "weekly_review_updates": weekly_updates,
        "calibration_manifest": calibration_manifest,
    }


def replay_forward_bridge_report_payload(
    *,
    bridge_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_REPLAY_FORWARD_BRIDGE_DIR,
) -> dict[str, Any]:
    bridge_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=bridge_id if not latest else None,
        pointer_name="latest_replay_forward_bridge",
    )
    return {
        **_read_json(bridge_dir / "bridge_manifest.json"),
        "forward_tracking_focus": _read_json(bridge_dir / "forward_tracking_focus.json"),
        "weekly_review_updates": _read_json(bridge_dir / "weekly_review_updates.json"),
        "bridge_dir": str(bridge_dir),
    }


def validate_replay_forward_bridge_artifact(
    *,
    bridge_id: str,
    output_dir: Path = DEFAULT_REPLAY_FORWARD_BRIDGE_DIR,
) -> dict[str, Any]:
    bridge_dir = output_dir / bridge_id
    manifest = _read_optional_json(bridge_dir / "bridge_manifest.json") or {}
    focus = _read_optional_json(bridge_dir / "forward_tracking_focus.json") or {}
    checks = [
        _check("manifest_exists", (bridge_dir / "bridge_manifest.json").exists(), ""),
        _check("focus_exists", (bridge_dir / "forward_tracking_focus.json").exists(), ""),
        _check("weekly_updates_exists", (bridge_dir / "weekly_review_updates.json").exists(), ""),
        _check("report_exists", (bridge_dir / "replay_forward_bridge_report.md").exists(), ""),
        _check("reader_brief_exists", (bridge_dir / "reader_brief_section.md").exists(), ""),
        _check("bridge_id_matches", manifest.get("bridge_id") == bridge_id, bridge_id),
        _check(
            "focus_items_present",
            bool(_records(focus.get("focus_items"))),
            "forward focus items",
        ),
        _check(
            "no_auto_policy_or_broker",
            manifest.get("production_candidate_generated") is False
            and manifest.get("automatic_candidate_promotion") is False
            and manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "bridge is observation-only",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_replay_forward_bridge_validation",
        artifact_id_key="bridge_id",
        artifact_id=bridge_id,
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
    hard_limitations = [
        row for row in rows if HARD_PIT_LIMITATIONS & set(_texts(row.get("replay_limitations")))
    ]
    hard_dates = ", ".join(sorted({_text(row.get("as_of")) for row in hard_limitations}))
    return "\n".join(
        [
            "# Dynamic v3 historical replay inventory",
            "",
            f"- 状态：{manifest.get('status')}",
            f"- inventory_id：`{manifest.get('inventory_id')}`",
            f"- 日期范围：{manifest.get('start')} to {manifest.get('end')}",
            f"- 实际来源范围：{manifest.get('actual_start') or 'none'} to "
            f"{manifest.get('actual_end') or 'none'}",
            f"- evidence cutoff：{manifest.get('evidence_cutoff')}",
            f"- market regime：{manifest.get('market_regime')}",
            f"- cutoff后来源排除数："
            f"{manifest.get('future_generated_source_excluded_count')}",
            f"- price data role：{manifest.get('price_data_role')}",
            f"- historical advisory events：{manifest.get('total_replay_events')}",
            f"- PIT_SAFE：{audit.get('pit_safe_count')}",
            f"- PIT_WARNING：{audit.get('pit_warning_count')}",
            f"- PIT_UNSAFE：{audit.get('pit_unsafe_count')}",
            f"- 可重放日期：{dates or 'none'}",
            f"- 缺少 target weights 日期：{missing_targets or 'none'}",
            f"- 缺少价格数据日期：{missing_prices or 'none'}",
            f"- hard PIT limitation events：{len(hard_limitations)}",
            f"- hard PIT limitation dates：{hard_dates or 'none'}",
            f"- eligible / partial / ineligible：{coverage.get('eligible_count')} / "
            f"{coverage.get('partial_count')} / {coverage.get('ineligible_count')}",
            "- 默认 replay 不允许 PIT_UNSAFE 进入 outcome 链路；"
            "--include-pit-warning 不会覆盖 hard PIT limitations。",
            "- future price rows只判断outcome availability，不进入decision input。",
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
    skipped_pit_unsafe = sum(
        1 for row in skipped if _text(row.get("pit_safety_status")) == "PIT_UNSAFE"
    )
    return "\n".join(
        [
            "# Dynamic v3 historical advisory replay",
            "",
            f"- 状态：{manifest.get('status')}",
            f"- replay_id：`{manifest.get('replay_id')}`",
            f"- replay events：{manifest.get('replay_event_count')}",
            f"- skipped events：{manifest.get('skipped_count')}",
            f"- generated variants：{variants}",
            f"- include_pit_warning：{manifest.get('include_pit_warning')}",
            f"- PIT_SAFE / PIT_WARNING / PIT_UNSAFE in replay："
            f"{pit_counts.get('PIT_SAFE', 0)} / {pit_counts.get('PIT_WARNING', 0)} / "
            f"{pit_counts.get('PIT_UNSAFE', 0)}",
            f"- skipped PIT_UNSAFE events：{skipped_pit_unsafe}",
            f"- skipped reasons：{dict(skipped_reasons) or 'none'}",
            f"- broker action present：{summary.get('broker_action_present')}",
            "- outcome_mode=HISTORICAL_REPLAY。",
            "- production_effect=none；broker_action_taken=false。",
            "",
        ]
    )


def render_backfill_outcome_report(manifest: Mapping[str, Any], summary: Mapping[str, Any]) -> str:
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


def render_replay_diagnosis_report(
    manifest: Mapping[str, Any],
    coverage: Mapping[str, Any],
    pending_reasons: Mapping[str, Any],
    artifact_health: Sequence[Mapping[str, Any]],
) -> str:
    inventory = _mapping(coverage.get("inventory"))
    replay = _mapping(coverage.get("replay"))
    backfill = _mapping(coverage.get("backfill"))
    review = _mapping(coverage.get("review"))
    reasons = _records(pending_reasons.get("pending_reasons"))
    top_reasons = ", ".join(f"{row.get('reason')}={row.get('count')}" for row in reasons[:5])
    return "\n".join(
        [
            f"# Replay Coverage Diagnosis {manifest.get('diagnosis_id')}",
            "",
            "## 结论",
            "",
            f"- diagnosis_status：{manifest.get('status')}",
            f"- can_enter_variant_comparison：{manifest.get('can_enter_variant_comparison')}",
            f"- top_pending_reasons：{top_reasons or 'MISSING'}",
            "",
            "## 覆盖率",
            "",
            f"- inventory_events：{inventory.get('total_events')}",
            f"- PIT_SAFE / PIT_WARNING / PIT_UNSAFE："
            f"{inventory.get('pit_safe')} / {inventory.get('pit_warning')} / "
            f"{inventory.get('pit_unsafe')}",
            f"- replayed_events / skipped_events："
            f"{replay.get('replayed_events')} / {replay.get('skipped_events')}",
            f"- AVAILABLE / PENDING / INSUFFICIENT_DATA："
            f"{backfill.get('available_windows')} / {backfill.get('pending_windows')} / "
            f"{backfill.get('insufficient_data_windows')}",
            f"- review_status：{review.get('review_status')}",
            "",
            "## Artifact Health",
            "",
            *[
                f"- {row.get('artifact_type')} {row.get('artifact_id')}: "
                f"exists={row.get('exists')}; status={row.get('status')}; "
                f"records={row.get('record_count')}"
                for row in artifact_health
            ],
            "",
            "## 判断",
            "",
            "- 如果 blocking reason 主要是 pit_unsafe 或 insufficient_replay_events，"
            "应扩展 replay inventory 或重新收集 PIT-safe source artifact。",
            "- 如果主要是 future_window_not_reached，"
            "应等待窗口到期或改用更早的 historical replay event。",
            "- 如果主要是 missing_price_data，应先修复 price cache，再解释 backfilled outcome。",
            "- 本报告只诊断，不修改 replay、backfill、policy、production 或 broker state。",
            "",
        ]
    )


def render_backfill_repair_report(
    manifest: Mapping[str, Any],
    delta: Mapping[str, Any],
    actions: Sequence[Mapping[str, Any]],
) -> str:
    before = _mapping(delta.get("before"))
    after = _mapping(delta.get("after"))
    action_counter = Counter(_text(row.get("repair_action")) for row in actions)
    return "\n".join(
        [
            f"# Backfill Repair {manifest.get('repair_id')}",
            "",
            f"- status：{manifest.get('status')}",
            f"- data_quality_status：{manifest.get('data_quality_status')}",
            f"- before AVAILABLE / PENDING / INSUFFICIENT_DATA："
            f"{before.get('available')} / {before.get('pending')} / "
            f"{before.get('insufficient_data')}",
            f"- after AVAILABLE / PENDING / INSUFFICIENT_DATA："
            f"{after.get('available')} / {after.get('pending')} / "
            f"{after.get('insufficient_data')}",
            f"- repaired_count：{delta.get('repaired_count')}",
            f"- still_pending_count：{delta.get('still_pending_count')}",
            f"- still_insufficient_count：{delta.get('still_insufficient_count')}",
            f"- repair_actions：{dict(sorted(action_counter.items()))}",
            "- future_data_used_in_decision：false",
            "- 不覆写原始 backfilled outcome，不修改 replay decision input。",
            "",
        ]
    )


def render_variant_comparison_report(
    manifest: Mapping[str, Any],
    metrics: Sequence[Mapping[str, Any]],
    pairwise: Mapping[str, Any],
    ranking: Mapping[str, Any],
) -> str:
    limited_pair = _limited_adjustment_vs_no_trade_pair(pairwise)
    return "\n".join(
        [
            f"# Variant Performance Comparison {manifest.get('comparison_id')}",
            "",
            f"- status：{manifest.get('status')}",
            f"- best_variant：{ranking.get('best_variant')}",
            f"- recommendation_confidence：{ranking.get('recommendation_confidence')}",
            f"- limited_adjustment_vs_no_trade："
            f"{limited_pair.get('limited_adjustment_conclusion', 'INSUFFICIENT_DATA')}",
            "",
            "## Window Metrics",
            "",
            *[
                f"- {row.get('variant')} {row.get('window_days')}d: "
                f"available={row.get('available_count')}; "
                f"avg_return={row.get('avg_return')}; "
                f"avg_relative_to_no_trade={row.get('avg_relative_to_no_trade')}; "
                f"status={row.get('status')}"
                for row in metrics
            ],
            "",
            "## 解读",
            "",
            "- 样本不足时标记 INSUFFICIENT_DATA，不生成强结论。",
            "- ranking 的 overall_score 等于 avg_relative_to_no_trade 的透明诊断值，"
            "不是 production policy score。",
            "- owner_decision 和 paper_action 只作为人工复核参考，"
            "不代表 broker 或 production action。",
            "",
        ]
    )


def _limited_adjustment_vs_no_trade_pair(pairwise: Mapping[str, Any]) -> dict[str, Any]:
    for row in _records(pairwise.get("comparisons")):
        variant_a = _text(row.get("variant_a"))
        variant_b = _text(row.get("variant_b"))
        if {variant_a, variant_b} != {"limited_adjustment", "no_trade"}:
            continue
        normalized = dict(row)
        conclusion = _text(row.get("conclusion"), "insufficient_data")
        if variant_a == "limited_adjustment":
            normalized["limited_adjustment_conclusion"] = conclusion
            normalized["limited_adjustment_avg_return_delta"] = row.get("avg_return_delta", 0.0)
        else:
            if conclusion == "variant_a_better":
                limited_conclusion = "no_trade_better"
            elif conclusion == "variant_b_better":
                limited_conclusion = "limited_adjustment_better"
            else:
                limited_conclusion = conclusion
            normalized["limited_adjustment_conclusion"] = limited_conclusion
            normalized["limited_adjustment_avg_return_delta"] = -_float(
                row.get("avg_return_delta")
            )
        return normalized
    return {
        "limited_adjustment_conclusion": "INSUFFICIENT_DATA",
        "limited_adjustment_avg_return_delta": 0.0,
    }


def render_rule_calibration_report(
    manifest: Mapping[str, Any],
    diagnostics: Mapping[str, Any],
    proposals: Mapping[str, Any],
    safety: Mapping[str, Any],
) -> str:
    proposal_rows = _records(proposals.get("proposals"))
    return "\n".join(
        [
            f"# Rule Calibration {manifest.get('calibration_id')}",
            "",
            f"- status：{manifest.get('status')}",
            f"- current_policy：{diagnostics.get('current_policy')}",
            f"- auto_apply：{safety.get('auto_apply')}",
            f"- owner_approval_required：{safety.get('owner_approval_required')}",
            f"- requires_forward_confirmation：{safety.get('requires_forward_confirmation')}",
            "",
            "## Diagnostics",
            "",
            *[
                f"- {key}：{value}"
                for key, value in sorted(_mapping(diagnostics.get("diagnostics")).items())
            ],
            "",
            "## Proposals",
            "",
            *[
                f"- {row.get('proposal_id')}：{row.get('change_type')}；"
                f"auto_apply={row.get('auto_apply')}；"
                f"requires_owner_approval={row.get('requires_owner_approval')}；"
                f"reason={row.get('reason')}"
                for row in proposal_rows
            ],
            "",
            "- 本报告只输出 proposal，不自动修改 position_advisory_v1.yaml。",
            "- production_effect=none；broker_action_allowed=false。",
            "",
        ]
    )


def render_replay_forward_bridge_report(
    manifest: Mapping[str, Any],
    focus: Mapping[str, Any],
    weekly_updates: Mapping[str, Any],
    proposals: Mapping[str, Any],
) -> str:
    proposal_types = ", ".join(
        row.get("change_type", "") for row in _records(proposals.get("proposals"))
    )
    return "\n".join(
        [
            f"# Replay-to-Forward Bridge {manifest.get('bridge_id')}",
            "",
            f"- historical_replay_status：{manifest.get('historical_replay_status')}",
            f"- best_variant：{manifest.get('best_variant')}",
            f"- calibration_confidence：{manifest.get('calibration_confidence')}",
            f"- forward_tracking_status：{focus.get('forward_tracking_status')}",
            f"- next_action：{manifest.get('next_action')}",
            f"- calibration_proposals：{proposal_types or 'MISSING'}",
            "",
            "## Forward Focus",
            "",
            *[
                f"- {row.get('item')}：priority={row.get('priority')}；"
                f"windows={row.get('tracking_windows')}；"
                f"required_future_events={row.get('required_future_events')}；"
                f"reason={row.get('reason')}"
                for row in _records(focus.get("focus_items"))
            ],
            "",
            "## Weekly Review Updates",
            "",
            *[f"- add_section：{item}" for item in _texts(weekly_updates.get("add_sections"))],
            *[
                f"- question：{item}"
                for item in _texts(weekly_updates.get("recommended_weekly_questions"))
            ],
            "",
            "- 该 bridge 只指导 forward tracking 和 weekly review，"
            "不修改 policy、不生产、不触发 broker。",
            "",
        ]
    )


def render_replay_forward_bridge_reader_brief(
    manifest: Mapping[str, Any], focus: Mapping[str, Any]
) -> str:
    focus_items = _records(focus.get("focus_items"))
    primary = focus_items[0] if focus_items else {}
    return "\n".join(
        [
            "## Dynamic Rescue Replay-to-Forward Bridge",
            "",
            f"- historical_replay_status: {manifest.get('historical_replay_status')}",
            f"- best_variant: {manifest.get('best_variant')}",
            f"- calibration_confidence: {manifest.get('calibration_confidence')}",
            f"- forward_tracking_focus: {primary.get('item', 'MISSING')}",
            f"- next_action: {manifest.get('next_action')}",
            "- production_effect: none",
            "- broker_action_taken: false",
            "",
        ]
    )


def _replay_diagnosis_coverage_breakdown(
    *,
    inventory_manifest: Mapping[str, Any],
    inventory_coverage: Mapping[str, Any],
    replay_manifest: Mapping[str, Any],
    replay_summary: Mapping[str, Any],
    backfill_manifest: Mapping[str, Any],
    sim_manifest: Mapping[str, Any],
    sim_summary: Mapping[str, Any],
    sim_event_count: int,
    review_manifest: Mapping[str, Any],
    calibration: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "inventory_id": inventory_manifest.get("inventory_id", ""),
        "replay_id": replay_manifest.get("replay_id", ""),
        "backfill_id": backfill_manifest.get("backfill_id", ""),
        "sim_id": sim_manifest.get("sim_id", ""),
        "review_id": review_manifest.get("review_id", ""),
        "inventory": {
            "total_events": inventory_manifest.get("total_replay_events", 0),
            "pit_safe": inventory_manifest.get("pit_safe_count", 0),
            "pit_warning": inventory_manifest.get("pit_warning_count", 0),
            "pit_unsafe": inventory_manifest.get("pit_unsafe_count", 0),
            "eligible": inventory_coverage.get("eligible_count", 0),
            "partial": inventory_coverage.get("partial_count", 0),
            "ineligible": inventory_coverage.get("ineligible_count", 0),
        },
        "replay": {
            "replayed_events": replay_manifest.get("replay_event_count", 0),
            "skipped_events": replay_manifest.get("skipped_count", 0),
            "generated_variants": sum(
                _int(value) for value in _mapping(replay_summary.get("variant_counts")).values()
            ),
        },
        "backfill": {
            "available_windows": backfill_manifest.get("available_count", 0),
            "pending_windows": backfill_manifest.get("pending_count", 0),
            "insufficient_data_windows": backfill_manifest.get("insufficient_data_count", 0),
        },
        "paper_sim": {
            "simulation_status": sim_summary.get(
                "simulation_status",
                sim_manifest.get("status", "MISSING"),
            ),
            "event_count": sim_event_count,
            "date_range": {
                "start": sim_summary.get("start_date", ""),
                "end": sim_summary.get("end_date", ""),
            },
        },
        "review": {
            "review_status": review_manifest.get("status", "MISSING"),
            "reason": [
                row.get("type")
                for row in _records(calibration.get("recommendations"))
                if row.get("type")
            ],
        },
    }


def _replay_pending_reason_summary(
    *,
    inventory_rows: Sequence[Mapping[str, Any]],
    replay_summary: Mapping[str, Any],
    backfill_manifest: Mapping[str, Any],
    outcome_rows: Sequence[Mapping[str, Any]],
    sim_summary: Mapping[str, Any],
    review_manifest: Mapping[str, Any],
) -> dict[str, Any]:
    counter: Counter[str] = Counter()
    for row in inventory_rows:
        limitations = set(_texts(row.get("replay_limitations")))
        if row.get("pit_safety_status") == "PIT_UNSAFE":
            counter["pit_unsafe"] += 1
        if "MISSING_TARGET_WEIGHTS" in limitations:
            counter["missing_target_weights"] += 1
        if "MISSING_PRICE_DATA" in limitations:
            counter["missing_price_data"] += 1
    if _int(replay_summary.get("replay_event_count")) == 0:
        counter["insufficient_replay_events"] += 1
    for row in outcome_rows:
        status = _text(row.get("outcome_status"))
        if status == "PENDING":
            counter["future_window_not_reached"] += 1
        elif status == "INSUFFICIENT_DATA":
            counter["missing_price_data"] += 1
    if _int(backfill_manifest.get("available_count")) == 0:
        counter["no_available_outcome_windows"] += 1
    if _text(sim_summary.get("simulation_status")) == "INSUFFICIENT_DATA":
        counter["paper_sim_insufficient_data"] += 1
    if (
        _text(review_manifest.get("status")) in {"PENDING", "INSUFFICIENT_DATA"}
        and _int(backfill_manifest.get("available_count")) == 0
    ):
        counter["review_waiting_for_backfill"] += 1
    if not counter:
        counter["unknown"] += 1
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_replay_pending_reason_summary",
        "pending_reasons": [
            {
                "reason": reason,
                "count": count,
                "blocking": reason
                in {
                    "missing_price_data",
                    "missing_target_weights",
                    "pit_unsafe",
                    "insufficient_replay_events",
                    "no_available_outcome_windows",
                    "paper_sim_insufficient_data",
                    "review_waiting_for_backfill",
                    "unknown",
                },
                "recommended_action": PENDING_REASON_ACTIONS[reason],
            }
            for reason, count in counter.most_common()
        ],
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _artifact_health_row(
    artifact_type: str,
    artifact_id: str,
    path: Path,
    payload: Mapping[str, Any],
    record_count: int,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "artifact_type": artifact_type,
        "artifact_id": artifact_id,
        "path": str(path),
        "exists": path.exists(),
        "status": payload.get("status", "MISSING"),
        "record_count": record_count,
        "production_effect": payload.get("production_effect", "none"),
        "broker_action_allowed": payload.get("broker_action_allowed", False),
        "broker_action_taken": payload.get("broker_action_taken", False),
        "production_candidate_generated": payload.get("production_candidate_generated", False),
    }


def _availability_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counter = Counter(_text(row.get("outcome_status")) for row in rows)
    return {
        "available": counter.get("AVAILABLE", 0),
        "pending": counter.get("PENDING", 0),
        "insufficient_data": counter.get("INSUFFICIENT_DATA", 0),
    }


def _repair_outcome_rows(
    *,
    original_rows: Sequence[Mapping[str, Any]],
    event_map: Mapping[str, Mapping[str, Any]],
    prices: pd.DataFrame,
    price_dates: Sequence[date],
    generated_date: date,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    recomputed: dict[tuple[str, int, str], Mapping[str, Any]] = {}
    for replay_event_id, event in event_map.items():
        windows = sorted(
            {
                _int(row.get("window_days"))
                for row in original_rows
                if _text(row.get("replay_event_id")) == replay_event_id
            }
        )
        for row in _backfilled_outcome_rows(
            event=event,
            windows=[window for window in windows if window > 0],
            prices=prices,
            price_dates=price_dates,
            generated_date=generated_date,
        ):
            key = (
                _text(row.get("replay_event_id")),
                _int(row.get("window_days")),
                _text(row.get("variant")),
            )
            recomputed[key] = row
    repaired_rows: list[dict[str, Any]] = []
    actions: list[dict[str, Any]] = []
    for original in original_rows:
        key = (
            _text(original.get("replay_event_id")),
            _int(original.get("window_days")),
            _text(original.get("variant")),
        )
        candidate = dict(recomputed.get(key, original))
        original_status = _text(original.get("outcome_status"), "MISSING")
        new_status = _text(candidate.get("outcome_status"), original_status)
        if original_status == "AVAILABLE":
            candidate = dict(original)
            new_status = "AVAILABLE"
            repair_action = "no_change"
            reason = "original window already available"
        elif key not in recomputed:
            candidate = dict(original)
            repair_action = "no_change"
            reason = "source replay event unavailable"
        elif original_status != new_status and new_status == "AVAILABLE":
            repair_action = "price_cache_lookup"
            reason = "price cache now contains enough historical prices for this window"
        elif original_status == "PENDING" and _text(original.get("end_date")) != _text(
            candidate.get("end_date")
        ):
            repair_action = "calendar_recompute"
            reason = "outcome window end date was recomputed from available trading dates"
        elif new_status == "INSUFFICIENT_DATA":
            repair_action = "price_date_alignment"
            reason = "window is due but price path remains incomplete"
        else:
            repair_action = "no_change"
            reason = "window remains pending or unchanged"
        repaired_rows.append(candidate)
        actions.append(
            {
                "replay_event_id": original.get("replay_event_id"),
                "variant": original.get("variant"),
                "window_days": original.get("window_days"),
                "original_status": original_status,
                "new_status": new_status,
                "repair_action": repair_action,
                "reason": reason,
                "future_data_used_in_decision": False,
            }
        )
    return repaired_rows, actions


def _variant_window_metrics(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for variant in REPLAY_VARIANTS:
        for window in OUTCOME_WINDOWS:
            window_rows = [
                row
                for row in rows
                if _text(row.get("variant")) == variant
                and _int(row.get("window_days")) == window
                and row.get("outcome_status") == "AVAILABLE"
            ]
            returns = [_float(row.get("return")) for row in window_rows]
            rel = [_float(row.get("relative_to_no_trade")) for row in window_rows]
            result.append(
                {
                    "variant": variant,
                    "window_days": window,
                    "available_count": len(window_rows),
                    "avg_return": round(_avg(returns), 6),
                    "median_return": round(_median(returns), 6),
                    "avg_relative_to_no_trade": round(_avg(rel), 6),
                    "median_relative_to_no_trade": round(_median(rel), 6),
                    "win_rate_vs_no_trade": (
                        round(sum(1 for value in rel if value > 0) / len(rel), 6) if rel else 0.0
                    ),
                    "avg_max_drawdown": round(
                        _avg([_float(row.get("max_drawdown")) for row in window_rows]),
                        6,
                    ),
                    "avg_realized_volatility": round(
                        _avg([_float(row.get("realized_volatility")) for row in window_rows]),
                        6,
                    ),
                    "avg_turnover": round(
                        _avg([_float(row.get("turnover")) for row in window_rows]),
                        6,
                    ),
                    "status": "PASS" if window_rows else "INSUFFICIENT_DATA",
                }
            )
    return result


def _variant_pairwise_comparison(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    available = [row for row in rows if row.get("outcome_status") == "AVAILABLE"]
    by_key = {
        (
            _text(row.get("replay_event_id")),
            _int(row.get("window_days")),
            _text(row.get("variant")),
        ): row
        for row in available
    }
    comparisons_out = []
    for variant_a, variant_b in combinations(REPLAY_VARIANTS, 2):
        for window in OUTCOME_WINDOWS:
            deltas = []
            drawdowns = []
            turnovers = []
            for event_id in sorted({_text(row.get("replay_event_id")) for row in available}):
                left = by_key.get((event_id, window, variant_a))
                right = by_key.get((event_id, window, variant_b))
                if not left or not right:
                    continue
                deltas.append(_float(left.get("return")) - _float(right.get("return")))
                drawdowns.append(
                    _float(left.get("max_drawdown")) - _float(right.get("max_drawdown"))
                )
                turnovers.append(_float(left.get("turnover")) - _float(right.get("turnover")))
            avg_delta = round(_avg(deltas), 6)
            drawdown_delta = round(_avg(drawdowns), 6)
            turnover_delta = round(_avg(turnovers), 6)
            conclusion = "insufficient_data"
            if deltas:
                if avg_delta > 0 and drawdown_delta >= 0:
                    conclusion = "variant_a_better"
                elif avg_delta < 0 and drawdown_delta <= 0:
                    conclusion = "variant_b_better"
                else:
                    conclusion = "mixed"
            comparisons_out.append(
                {
                    "variant_a": variant_a,
                    "variant_b": variant_b,
                    "window_days": window,
                    "event_count": len(deltas),
                    "avg_return_delta": avg_delta,
                    "win_rate": (
                        round(sum(1 for value in deltas if value > 0) / len(deltas), 6)
                        if deltas
                        else 0.0
                    ),
                    "drawdown_delta": drawdown_delta,
                    "turnover_delta": turnover_delta,
                    "conclusion": conclusion,
                }
            )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_variant_pairwise_comparison",
        "comparisons": comparisons_out,
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _variant_rank_summary(metrics: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows = []
    for variant in REPLAY_VARIANTS:
        variant_rows = [row for row in metrics if row.get("variant") == variant]
        available = [row for row in variant_rows if _int(row.get("available_count")) > 0]
        avg_relative = _avg([_float(row.get("avg_relative_to_no_trade")) for row in available])
        avg_return = _avg([_float(row.get("avg_return")) for row in available])
        avg_drawdown = _avg([_float(row.get("avg_max_drawdown")) for row in available])
        avg_turnover = _avg([_float(row.get("avg_turnover")) for row in available])
        rows.append(
            {
                "variant": variant,
                "overall_score": round(avg_relative, 6),
                "return_score": round(avg_return, 6),
                "drawdown_score": round(avg_drawdown, 6),
                "stability_score": sum(_int(row.get("available_count")) for row in available),
                "turnover_penalty": round(-avg_turnover, 6),
                "status": "PASS_WITH_WARNINGS" if available else "INSUFFICIENT_DATA",
            }
        )
    ranked = sorted(
        [row for row in rows if row["status"] != "INSUFFICIENT_DATA"],
        key=lambda row: (row["overall_score"], row["return_score"], row["drawdown_score"]),
        reverse=True,
    )
    ranking = [{**row, "rank": index + 1} for index, row in enumerate(ranked)]
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_variant_rank_summary",
        "ranking": ranking,
        "best_variant": ranking[0]["variant"] if ranking else "MISSING",
        "recommendation_confidence": "LOW" if ranking else "INSUFFICIENT_DATA",
        "ranking_method": "overall_score_is_avg_relative_to_no_trade_diagnostic",
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _advisory_rule_diagnostics_from_comparison(
    comparison_manifest: Mapping[str, Any],
    ranking: Mapping[str, Any],
    metrics: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    best = _text(ranking.get("best_variant"), "MISSING")
    confidence = _text(ranking.get("recommendation_confidence"), "INSUFFICIENT_DATA")
    limited = _metric_for(metrics, "limited_adjustment", 5)
    consensus = _metric_for(metrics, "consensus_target", 5)
    no_trade = _metric_for(metrics, "no_trade", 5)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_advisory_rule_diagnostics",
        "current_policy": "position_advisory_v1.yaml",
        "diagnostics": {
            "monitor_too_conservative": best not in {"MISSING", "no_trade"}
            and confidence != "INSUFFICIENT_DATA",
            "manual_review_too_frequent": False,
            "limited_adjustment_supported": best == "limited_adjustment",
            "consensus_target_too_aggressive": (
                _float(consensus.get("avg_max_drawdown")) < _float(no_trade.get("avg_max_drawdown"))
            ),
            "candidate_disagreement_rule_effective": True,
            "insufficient_data": confidence == "INSUFFICIENT_DATA",
        },
        "evidence": [
            {
                "comparison_id": comparison_manifest.get("comparison_id"),
                "best_variant": best,
                "recommendation_confidence": confidence,
                "limited_adjustment_avg_relative_to_no_trade_5d": limited.get(
                    "avg_relative_to_no_trade",
                    0.0,
                ),
                "consensus_target_avg_relative_to_no_trade_5d": consensus.get(
                    "avg_relative_to_no_trade",
                    0.0,
                ),
            }
        ],
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _policy_adjustment_proposals_from_diagnostics(
    comparison_manifest: Mapping[str, Any],
    ranking: Mapping[str, Any],
    diagnostics: Mapping[str, Any],
) -> dict[str, Any]:
    best = _text(ranking.get("best_variant"), "MISSING")
    confidence = _text(ranking.get("recommendation_confidence"), "INSUFFICIENT_DATA")
    diag = _mapping(diagnostics.get("diagnostics"))
    if confidence == "INSUFFICIENT_DATA":
        proposal = {
            "proposal_id": "require_more_forward_data",
            "change_type": "require_more_forward_data",
            "reason": "historical replay comparison has insufficient available outcome windows",
            "expected_effect": (
                "avoid over-calibrating advisory rules from unavailable replay outcomes"
            ),
        }
    elif best == "no_trade":
        proposal = {
            "proposal_id": "tighten_adjustment_when_no_trade_leads",
            "change_type": "tighten_adjustment",
            "reason": "no_trade ranks first in available historical replay comparison",
            "expected_effect": (
                "reduce adjustment frequency until forward outcomes improve evidence"
            ),
        }
    elif best == "limited_adjustment":
        proposal = {
            "proposal_id": "keep_current_limited_adjustment_watch",
            "change_type": "keep_current_rules",
            "reason": "limited_adjustment ranks first in available historical replay comparison",
            "expected_effect": "keep current rules while collecting forward confirmation",
        }
    elif diag.get("consensus_target_too_aggressive"):
        proposal = {
            "proposal_id": "increase_consensus_requirement",
            "change_type": "increase_consensus_requirement",
            "reason": "consensus_target shows weaker drawdown profile than no_trade",
            "expected_effect": "require stronger consensus before full target moves",
        }
    else:
        proposal = {
            "proposal_id": "continue_forward_tracking",
            "change_type": "require_more_forward_data",
            "reason": f"best variant is {best}; forward confirmation is still required",
            "expected_effect": "defer policy mutation until FORWARD_OUTCOME evidence arrives",
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_proposed_policy_adjustments",
        "comparison_id": comparison_manifest.get("comparison_id"),
        "proposals": [
            {
                **proposal,
                "affected_config": "position_advisory_v1.yaml",
                "current_value": None,
                "proposed_value": None,
                "risk": "historical replay evidence may be sparse or regime-specific",
                "requires_owner_approval": True,
                "auto_apply": False,
            }
        ],
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _forward_tracking_focus_from_replay(
    *,
    diagnosis_manifest: Mapping[str, Any],
    pending_reasons: Mapping[str, Any],
    comparison_manifest: Mapping[str, Any],
    ranking: Mapping[str, Any],
    proposals: Mapping[str, Any],
) -> dict[str, Any]:
    confidence = _text(
        comparison_manifest.get("recommendation_confidence"),
        _text(ranking.get("recommendation_confidence"), "INSUFFICIENT_DATA"),
    )
    proposal_types = [_text(row.get("change_type")) for row in _records(proposals.get("proposals"))]
    status = "CONTINUE"
    if confidence == "INSUFFICIENT_DATA":
        status = "INSUFFICIENT_DATA"
    elif "require_more_forward_data" in proposal_types:
        status = "INCREASE_FOCUS"
    top_reason = _records(pending_reasons.get("pending_reasons"))
    reason_text = top_reason[0]["reason"] if top_reason else "unknown"
    next_actions = proposal_types or ["continue_forward_tracking"]
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_forward_tracking_focus",
        "focus_items": [
            {
                "item": "limited_adjustment_vs_no_trade",
                "priority": "HIGH",
                "reason": (
                    "historical replay needs forward confirmation for limited adjustment edge"
                ),
                "tracking_windows": list(OUTCOME_WINDOWS),
                "required_future_events": FORWARD_CONFIRMATION_REQUIRED_EVENTS,
            },
            {
                "item": "pending_reason_resolution",
                "priority": "HIGH" if diagnosis_manifest.get("status") != "PASS" else "MEDIUM",
                "reason": f"top replay diagnosis reason is {reason_text}",
                "tracking_windows": list(OUTCOME_WINDOWS),
                "required_future_events": FORWARD_CONFIRMATION_REQUIRED_EVENTS,
            },
            {
                "item": "consensus_target_risk",
                "priority": "MEDIUM",
                "reason": "consensus_target return and drawdown need weekly review visibility",
                "tracking_windows": list(OUTCOME_WINDOWS),
                "required_future_events": FORWARD_CONFIRMATION_REQUIRED_EVENTS,
            },
        ],
        "forward_tracking_status": status,
        "next_actions": next_actions,
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _weekly_review_updates_from_focus(focus: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_review_updates",
        "add_sections": [
            "Historical Replay Performance",
            "Replay Calibration Watchlist",
            "Forward Confirmation Targets",
        ],
        "recommended_weekly_questions": [
            "Did limited_adjustment outperform no_trade in new forward outcomes?",
            "Did high consensus remain predictive?",
            "Did manual_review frequency decrease?",
            "Were prior PENDING or INSUFFICIENT_DATA replay reasons resolved?",
        ],
        "forward_tracking_status": focus.get("forward_tracking_status", "MISSING"),
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _metric_for(metrics: Sequence[Mapping[str, Any]], variant: str, window: int) -> dict[str, Any]:
    return next(
        (
            dict(row)
            for row in metrics
            if row.get("variant") == variant and _int(row.get("window_days")) == window
        ),
        {},
    )


def _median(values: Sequence[float]) -> float:
    clean = sorted(value for value in values if pd.notna(value))
    if not clean:
        return 0.0
    midpoint = len(clean) // 2
    if len(clean) % 2:
        return clean[midpoint]
    return (clean[midpoint - 1] + clean[midpoint]) / 2


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
    generated_cutoff: datetime,
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
        generated_cutoff=generated_cutoff,
    )
    owner_review = _owner_review_for_daily(daily_advisory_id, owner_reviews)
    paper_action_weights = _paper_action_weights_for_daily(
        daily_advisory_id=daily_advisory_id,
        paper_portfolio_dir=paper_portfolio_dir,
        generated_cutoff=generated_cutoff,
    )
    limited_weights = _limited_adjustment_weights(current_weights, consensus_weights, config)
    source_shadow_monitor_id = _text(manifest.get("source_shadow_monitor_run_id"))
    consensus_drift = _consensus_drift_for_monitor(
        source_shadow_monitor_id,
        consensus_drift_dir=consensus_drift_dir,
        generated_cutoff=generated_cutoff,
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
            "shadow_monitor_manifest_path": (
                str(
                    shadow_monitor_run_dir
                    / source_shadow_monitor_id
                    / "shadow_monitor_manifest.json"
                )
                if source_shadow_monitor_id
                else ""
            ),
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
    missing = [variant["variant"] for variant in variants if not _mapping(variant.get("weights"))]
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
                    "relative_to_no_trade": (
                        round(
                            metrics["return"]
                            - returns.get("no_trade", _missing_metrics())["return"],
                            6,
                        )
                        if status == "AVAILABLE"
                        else 0.0
                    ),
                    "relative_to_consensus_target": (
                        round(
                            metrics["return"]
                            - returns.get("consensus_target", _missing_metrics())["return"],
                            6,
                        )
                        if status == "AVAILABLE"
                        else 0.0
                    ),
                    "relative_to_limited_adjustment": (
                        round(
                            metrics["return"]
                            - returns.get("limited_adjustment", _missing_metrics())["return"],
                            6,
                        )
                        if status == "AVAILABLE"
                        else 0.0
                    ),
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
                "false_alarm_rate": (
                    round(sum(1 for value in rel_5 if value <= 0) / len(rel_5), 6) if rel_5 else 0.0
                ),
                "missed_opportunity_rate": (
                    round(sum(1 for value in rel_5 if value > 0) / len(rel_5), 6) if rel_5 else 0.0
                ),
            }
        )
    variant_effectiveness = [
        {
            "variant": row.get("variant"),
            "win_rate_vs_no_trade": row.get("win_rate_vs_no_trade_5d", 0.0),
            "avg_return_delta": row.get("avg_relative_to_no_trade_5d", 0.0),
            "drawdown_delta": row.get("avg_max_drawdown_20d", 0.0),
            "turnover": (
                sim_summary.get("turnover", 0.0)
                if row.get("variant") == sim_summary.get("variant")
                else row.get("avg_turnover", 0.0)
            ),
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
                "win_rate_vs_no_trade_5d": (
                    round(sum(1 for value in rel_5 if value > 0) / len(rel_5), 6) if rel_5 else 0.0
                ),
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
    *,
    advisory_dir: Path,
    as_of: date,
    paper_portfolio_dir: Path,
    generated_cutoff: datetime,
) -> tuple[dict[str, float], list[str]]:
    limitations = []
    for row in _read_jsonl(advisory_dir / "daily_position_deltas.jsonl"):
        current = _mapping(row.get("current_weights"))
        if current:
            return _normalize_weights(current), limitations
    paper = _paper_weights_at_or_before(
        as_of=as_of,
        paper_portfolio_dir=paper_portfolio_dir,
        generated_cutoff=generated_cutoff,
    )
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


def _paper_weights_at_or_before(
    *,
    as_of: date,
    paper_portfolio_dir: Path,
    generated_cutoff: datetime,
) -> dict[str, float]:
    candidates: list[tuple[date, datetime, dict[str, Any]]] = []
    for path in paper_portfolio_dir.glob("*/paper_position_history.jsonl"):
        for row in _read_jsonl(path):
            row_date = _date_from_any(row.get("as_of"))
            created_at = _datetime_from_any(row.get("created_at"))
            if row_date is None or created_at is None:
                raise DynamicV3HistoricalReplayError(
                    f"paper position history time is invalid: {path}"
                )
            if row_date <= as_of and created_at <= generated_cutoff:
                candidates.append((row_date, created_at, row))
    if not candidates:
        return {}
    latest = sorted(candidates, key=lambda item: (item[0], item[1]))[-1][2]
    return _normalize_weights(_mapping(latest.get("positions")))


def _owner_reviews_at_cutoff(
    path: Path,
    *,
    generated_cutoff: datetime,
) -> list[dict[str, Any]]:
    selected = []
    selected_daily_ids: set[str] = set()
    for row in _read_jsonl(path):
        created_at = _datetime_from_any(row.get("created_at"))
        updated_at = _datetime_from_any(row.get("updated_at") or row.get("created_at"))
        if created_at is None or updated_at is None or updated_at < created_at:
            raise DynamicV3HistoricalReplayError(
                f"owner review journal time is invalid: {path}"
            )
        if updated_at > generated_cutoff:
            continue
        daily_id = _text(row.get("daily_advisory_id"))
        if not daily_id or daily_id in selected_daily_ids:
            raise DynamicV3HistoricalReplayError(
                f"owner review cutoff selection is ambiguous: {daily_id or 'MISSING'}"
            )
        selected_daily_ids.add(daily_id)
        selected.append(dict(row))
    return selected


def _paper_action_weights_for_daily(
    *,
    daily_advisory_id: str,
    paper_portfolio_dir: Path,
    generated_cutoff: datetime,
) -> dict[str, Any]:
    candidates = []
    for path in paper_portfolio_dir.glob("*/paper_action_ledger.jsonl"):
        for row in _read_jsonl(path):
            if _text(row.get("daily_advisory_id")) == daily_advisory_id:
                created_at = _datetime_from_any(row.get("created_at"))
                if created_at is None:
                    raise DynamicV3HistoricalReplayError(
                        f"paper action created_at is invalid: {path}"
                    )
                if created_at <= generated_cutoff:
                    candidates.append((created_at, row, path))
    if not candidates:
        return {}
    latest_at = max(item[0] for item in candidates)
    latest = [item for item in candidates if item[0] == latest_at]
    if len(latest) != 1:
        raise DynamicV3HistoricalReplayError(
            f"paper action cutoff selection is ambiguous: {daily_advisory_id}"
        )
    _, row, path = latest[0]
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
    if len(matches) > 1:
        raise DynamicV3HistoricalReplayError(
            f"owner review cutoff selection is ambiguous: {daily_advisory_id}"
        )
    return matches[0] if matches else {}


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
    shadow_monitor_run_id: str,
    *,
    consensus_drift_dir: Path,
    generated_cutoff: datetime,
) -> dict[str, Any]:
    if not shadow_monitor_run_id:
        return {}
    candidates = []
    for child in consensus_drift_dir.glob("*"):
        if not child.is_dir():
            continue
        manifest = _read_optional_json(child / "consensus_drift_manifest.json") or {}
        if _text(manifest.get("source_shadow_monitor_run_id")) != shadow_monitor_run_id:
            continue
        generated_at = _datetime_from_any(manifest.get("generated_at"))
        if generated_at is None:
            raise DynamicV3HistoricalReplayError(
                f"consensus drift generated_at is invalid: {child}"
            )
        if generated_at <= generated_cutoff:
            candidates.append((generated_at, child))
    if not candidates:
        return {}
    latest_at = max(item[0] for item in candidates)
    latest_candidates = [item[1] for item in candidates if item[0] == latest_at]
    if len(latest_candidates) != 1:
        raise DynamicV3HistoricalReplayError(
            f"consensus drift cutoff selection is ambiguous: {shadow_monitor_run_id}"
        )
    latest = latest_candidates[0]
    manifest = _read_optional_json(latest / "consensus_drift_manifest.json") or {}
    summary = _read_optional_json(latest / "consensus_drift_summary.json") or {}
    return {
        **summary,
        "drift_id": manifest.get("drift_id", latest.name),
        "manifest_path": str(latest / "consensus_drift_manifest.json"),
    }


def _pit_status_and_eligibility(limitations: Sequence[str]) -> tuple[str, str]:
    limitation_set = set(limitations)
    if HARD_PIT_LIMITATIONS & limitation_set:
        return "PIT_UNSAFE", "INELIGIBLE"
    if WARNING_PIT_LIMITATIONS & limitation_set:
        return "PIT_WARNING", "PARTIAL"
    return "PIT_SAFE", "ELIGIBLE"


def _pit_safety_audit(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    counter = Counter(_text(row.get("pit_safety_status")) for row in rows)
    limitations = Counter(
        limitation for row in rows for limitation in _texts(row.get("replay_limitations"))
    )
    hard_limitations = {
        limitation: count
        for limitation, count in sorted(limitations.items())
        if limitation in HARD_PIT_LIMITATIONS
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_replay_pit_safety_audit",
        "status": "PASS" if not counter.get("PIT_UNSAFE") else "PASS_WITH_WARNINGS",
        "total_replay_events": len(rows),
        "pit_safe_count": counter.get("PIT_SAFE", 0),
        "pit_warning_count": counter.get("PIT_WARNING", 0),
        "pit_unsafe_count": counter.get("PIT_UNSAFE", 0),
        "hard_pit_limitation_count": sum(hard_limitations.values()),
        "hard_pit_limitations": hard_limitations,
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
    skipped_reason_counter = Counter(_text(row.get("skip_reason")) for row in skipped)
    variant_counter = Counter(
        _text(variant.get("variant")) for row in events for variant in _records(row.get("variants"))
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_replay_action_summary",
        "replay_event_count": len(events),
        "skipped_count": len(skipped),
        "recommended_action_counts": dict(sorted(action_counter.items())),
        "skipped_reason_counts": dict(sorted(skipped_reason_counter.items())),
        "skipped_events": list(skipped),
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
        start_price = _portfolio_price(pivot, start_idx, symbol)
        end_price = _portfolio_price(pivot, end, symbol)
        total_return += _float(weight) * (end_price / start_price - 1.0)
    daily_returns = []
    for left, right in zip(path_dates, path_dates[1:], strict=False):
        day_return = 0.0
        for symbol, weight in clean.items():
            if symbol == "CASH":
                continue
            left_price = _portfolio_price(pivot, left, symbol)
            right_price = _portfolio_price(pivot, right, symbol)
            day_return += _float(weight) * (right_price / left_price - 1.0)
        daily_returns.append(day_return)
    return total_return, daily_returns


def _portfolio_price(pivot: pd.DataFrame, price_date: date, symbol: str) -> float:
    if symbol not in pivot.columns:
        raise DynamicV3HistoricalReplayError(f"missing symbol price: {symbol}")
    try:
        value = _float(pivot.loc[price_date, symbol], default=float("nan"))
    except KeyError as exc:
        raise DynamicV3HistoricalReplayError(
            f"missing price date for {symbol}: {price_date.isoformat()}"
        ) from exc
    if not math.isfinite(value) or value <= 0:
        raise DynamicV3HistoricalReplayError(
            f"invalid price for {symbol}: {price_date.isoformat()}"
        )
    return value


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
