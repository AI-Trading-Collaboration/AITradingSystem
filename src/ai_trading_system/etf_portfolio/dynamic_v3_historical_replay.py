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
import yaml

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
HISTORICAL_REPLAY_SNAPSHOT_SCHEMA_VERSION = "historical_replay_source_snapshot.v2"
BACKFILLED_OUTCOME_SNAPSHOT_SCHEMA_VERSION = "backfilled_outcome_source_snapshot.v2"
HISTORICAL_PAPER_SIM_SNAPSHOT_SCHEMA_VERSION = "historical_paper_sim_source_snapshot.v2"
REPLAY_PERFORMANCE_REVIEW_SNAPSHOT_SCHEMA_VERSION = "replay_performance_review_source_snapshot.v2"
REPLAY_DIAGNOSIS_SNAPSHOT_SCHEMA_VERSION = "replay_diagnosis_source_snapshot.v2"
BACKFILL_REPAIR_SNAPSHOT_SCHEMA_VERSION = "backfill_repair_source_snapshot.v2"
DEFAULT_REPLAY_PERFORMANCE_REVIEW_POLICY_PATH = Path(
    "config/etf_portfolio/dynamic_v3_rescue/replay_performance_review_v1.yaml"
)

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
            _text(manifest.get("daily_advisory_id")) for _, manifest, _ in selected_advisories
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
        if as_of is None or _text(manifest.get("daily_advisory_id")) != _text(
            source.get("daily_advisory_id")
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
    report_matches = (
        report_path.is_file() and report_path.read_text(encoding="utf-8") == expected_report
    )
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
            _int(manifest.get("pit_unsafe_count")) == _int(expected_audit.get("pit_unsafe_count")),
            _int(manifest.get("eligible_count")) == _int(expected_coverage.get("eligible_count")),
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
            manifest.get("price_data_role") == "outcome_availability_only_not_decision_input",
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
    generated = _require_aware_utc(generated_at or datetime.now(UTC), "generated_at")
    source_dir = inventory_dir / inventory_id
    inventory_validation = validate_replay_inventory_artifact(
        inventory_id=inventory_id,
        output_dir=inventory_dir,
    )
    if inventory_validation.get("status") != "PASS":
        raise DynamicV3HistoricalReplayError(
            "historical replay requires a fully validated snapshotted inventory"
        )
    inventory_manifest = _read_json(source_dir / "replay_inventory_manifest.json")
    inventory_rows = _read_jsonl(source_dir / "replay_artifact_inventory.jsonl")
    inventory_snapshot_path = source_dir / "replay_inventory_source_snapshot.json"
    inventory_snapshot = _read_json(inventory_snapshot_path)
    inventory_cutoff = _datetime_from_any(inventory_manifest.get("evidence_cutoff"))
    if inventory_cutoff is None or generated < inventory_cutoff:
        raise DynamicV3HistoricalReplayError(
            "historical replay generated_at cannot precede inventory evidence cutoff"
        )
    events, decision_inputs, skipped = _historical_replay_views(
        inventory_rows,
        include_pit_warning=include_pit_warning,
    )
    replay_id = _stable_id(
        "historical-replay",
        inventory_id,
        include_pit_warning,
        generated.isoformat(),
    )
    replay_dir = _unique_dir(output_dir / replay_id)
    replay_dir.mkdir(parents=True, exist_ok=False)
    action_summary = _replay_action_summary(events, skipped)
    replay_source_snapshot = {
        "schema_version": HISTORICAL_REPLAY_SNAPSHOT_SCHEMA_VERSION,
        "inventory_id": inventory_id,
        "inventory_root": str(source_dir),
        "inventory_manifest_checksum": _sha256_file(source_dir / "replay_inventory_manifest.json"),
        "inventory_source_snapshot_checksum": _sha256_file(inventory_snapshot_path),
        "inventory_manifest": inventory_manifest,
        "inventory_source_snapshot": inventory_snapshot,
        "inventory_rows": inventory_rows,
        "include_pit_warning": include_pit_warning,
        "generated_at": generated.isoformat(),
        "selected_daily_advisory_ids": [_text(event.get("daily_advisory_id")) for event in events],
        "skipped_events": skipped,
        "production_effect": "none",
        "broker_action_taken": False,
    }
    replay_source_snapshot_path = replay_dir / "historical_replay_source_snapshot.json"
    _write_json(replay_source_snapshot_path, replay_source_snapshot)
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
        "source_inventory_validation_status": "PASS",
        "source_inventory_evidence_cutoff": inventory_cutoff.isoformat(),
        "market_regime": inventory_manifest.get("market_regime"),
        "historical_replay_source_snapshot_path": str(replay_source_snapshot_path),
        "historical_replay_source_snapshot_checksum": _sha256_file(replay_source_snapshot_path),
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
    decision_inputs = _read_jsonl(replay_dir / "replay_decision_inputs.jsonl")
    action_summary = _read_optional_json(replay_dir / "replay_action_summary.json") or {}
    snapshot_path = replay_dir / "historical_replay_source_snapshot.json"
    if not snapshot_path.is_file():
        legacy_checks = _historical_replay_shallow_checks(
            replay_id=replay_id,
            replay_dir=replay_dir,
            manifest=manifest,
            events=events,
        )
        payload = _validation_payload(
            report_type="etf_dynamic_v3_historical_replay_validation",
            artifact_id_key="replay_id",
            artifact_id=replay_id,
            checks=legacy_checks,
        )
        if payload["status"] == "PASS":
            payload["status"] = "PASS_WITH_WARNINGS"
        payload["source_snapshot_status"] = "LEGACY_UNSNAPSHOTTED"
        return payload
    snapshot = _read_optional_json(snapshot_path) or {}
    source_root = Path(_text(snapshot.get("inventory_root")))
    source_inventory_id = _text(snapshot.get("inventory_id"))
    source_manifest_path = source_root / "replay_inventory_manifest.json"
    source_snapshot_path = source_root / "replay_inventory_source_snapshot.json"
    try:
        expected_events, expected_inputs, expected_skipped = _historical_replay_views(
            _records(snapshot.get("inventory_rows")),
            include_pit_warning=bool(snapshot.get("include_pit_warning")),
        )
        replay_error = ""
    except Exception as exc:  # noqa: BLE001
        expected_events, expected_inputs, expected_skipped = [], [], []
        replay_error = str(exc)
    expected_summary = _replay_action_summary(expected_events, expected_skipped)
    expected_report = render_historical_replay_report(
        manifest,
        expected_summary,
        expected_events,
        expected_skipped,
    )
    report_path = replay_dir / "historical_replay_report.md"
    source_validation = validate_replay_inventory_artifact(
        inventory_id=source_inventory_id,
        output_dir=source_root.parent,
    )
    derived_manifest_matches = all(
        (
            manifest.get("inventory_id") == source_inventory_id,
            manifest.get("generated_at") == snapshot.get("generated_at"),
            manifest.get("include_pit_warning") == snapshot.get("include_pit_warning"),
            _int(manifest.get("replay_event_count")) == len(expected_events),
            _int(manifest.get("skipped_count")) == len(expected_skipped),
            manifest.get("generated_variants") == list(REPLAY_VARIANTS),
            manifest.get("source_inventory_validation_status") == "PASS",
            manifest.get("market_regime")
            == _mapping(snapshot.get("inventory_manifest")).get("market_regime"),
        )
    )
    checks = [
        *_historical_replay_shallow_checks(
            replay_id=replay_id,
            replay_dir=replay_dir,
            manifest=manifest,
            events=events,
        ),
        _check(
            "source_snapshot_schema_valid",
            snapshot.get("schema_version") == HISTORICAL_REPLAY_SNAPSHOT_SCHEMA_VERSION,
            HISTORICAL_REPLAY_SNAPSHOT_SCHEMA_VERSION,
        ),
        _check(
            "source_snapshot_checksum_matches",
            manifest.get("historical_replay_source_snapshot_checksum")
            == _sha256_file(snapshot_path),
            "replay source snapshot",
        ),
        _check(
            "source_inventory_validation_passes",
            source_validation.get("status") == "PASS",
            source_inventory_id,
        ),
        _check(
            "source_inventory_files_unchanged",
            source_manifest_path.is_file()
            and source_snapshot_path.is_file()
            and snapshot.get("inventory_manifest_checksum") == _sha256_file(source_manifest_path)
            and snapshot.get("inventory_source_snapshot_checksum")
            == _sha256_file(source_snapshot_path),
            source_inventory_id,
        ),
        _check(
            "embedded_inventory_content_matches_source",
            source_manifest_path.is_file()
            and source_snapshot_path.is_file()
            and snapshot.get("inventory_manifest") == _read_json(source_manifest_path)
            and snapshot.get("inventory_source_snapshot") == _read_json(source_snapshot_path)
            and _records(snapshot.get("inventory_rows"))
            == _read_jsonl(source_root / "replay_artifact_inventory.jsonl"),
            source_inventory_id,
        ),
        _check("events_recomputed", not replay_error and events == expected_events, replay_error),
        _check(
            "decision_inputs_recomputed",
            not replay_error and decision_inputs == expected_inputs,
            replay_error,
        ),
        _check("action_summary_recomputed", action_summary == expected_summary, "summary"),
        _check("manifest_derived_fields_match", derived_manifest_matches, "manifest"),
        _check(
            "report_recomputed",
            report_path.is_file() and report_path.read_text(encoding="utf-8") == expected_report,
            "Markdown report",
        ),
        _check(
            "variant_weights_and_turnover_valid",
            not replay_error
            and all(
                variant.get("turnover_convention") == "one_way_l1_weight_change"
                and bool(variant.get("source_status"))
                for event in expected_events
                for variant in _records(event.get("variants"))
            ),
            replay_error or "variant contract",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_historical_replay_validation",
        artifact_id_key="replay_id",
        artifact_id=replay_id,
        checks=checks,
    )


def _historical_replay_shallow_checks(
    *,
    replay_id: str,
    replay_dir: Path,
    manifest: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        _check("manifest_exists", (replay_dir / "historical_replay_manifest.json").is_file(), ""),
        _check("replay_events_exists", (replay_dir / "replay_events.jsonl").is_file(), ""),
        _check(
            "decision_inputs_exists",
            (replay_dir / "replay_decision_inputs.jsonl").is_file(),
            "",
        ),
        _check("action_summary_exists", (replay_dir / "replay_action_summary.json").is_file(), ""),
        _check("report_exists", (replay_dir / "historical_replay_report.md").is_file(), ""),
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
                {variant.get("variant") for variant in _records(row.get("variants"))}
                == set(REPLAY_VARIANTS)
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
    generated = _require_aware_utc(generated_at or datetime.now(UTC), "generated_at")
    source_dir = replay_dir / replay_id
    source_validation = validate_historical_replay_artifact(
        replay_id=replay_id,
        output_dir=replay_dir,
    )
    if source_validation.get("status") != "PASS":
        raise DynamicV3HistoricalReplayError(
            "backfill outcome requires a fully validated snapshotted replay"
        )
    replay_manifest = _read_json(source_dir / "historical_replay_manifest.json")
    replay_generated = _datetime_from_any(replay_manifest.get("generated_at"))
    if replay_generated is None or generated < replay_generated:
        raise DynamicV3HistoricalReplayError(
            "backfill generated_at cannot precede historical replay generated_at"
        )
    replay_events = _read_jsonl(source_dir / "replay_events.jsonl")
    config = load_paper_portfolio_config(config_path)
    windows = _configured_outcome_windows(config)
    cost_rate = _backfill_cost_rate(config)
    policy_metadata = _mapping(config.get("policy_metadata"))
    quality = None
    if enforce_data_quality_gate:
        quality = _validate_cached_data_quality(
            as_of=generated.date(),
            prices_path=prices_path,
            rates_path=rates_path,
        )
        if not quality.passed:
            raise DynamicV3HistoricalReplayError(
                f"backfill outcome data quality gate failed: {quality.status}"
            )
    prices = _load_prices_for_replay(prices_path, replay_events)
    price_rows = _frozen_replay_price_rows(prices, generated_date=generated.date())
    frozen_prices = pd.DataFrame(
        price_rows,
        columns=["symbol", "date", "adj_close"],
    ).rename(columns={"date": "_date", "adj_close": "_adj_close"})
    frozen_prices["_date"] = pd.to_datetime(frozen_prices["_date"]).dt.date
    rows, summary, rollup, status = _backfill_views(
        replay_events,
        windows=windows,
        prices=frozen_prices,
        generated_date=generated.date(),
        cost_rate=cost_rate,
    )
    backfill_id = _stable_id("backfill-outcome", replay_id, generated.isoformat())
    backfill_dir = _unique_dir(output_dir / backfill_id)
    backfill_dir.mkdir(parents=True, exist_ok=False)
    quality_status = "SKIPPED_EXPLICIT_TEST_FIXTURE"
    quality_report_path = ""
    if enforce_data_quality_gate:
        quality_report = backfill_dir / "validate_data_quality_report.md"
        assert quality is not None
        write_data_quality_report(quality, quality_report)
        quality_status = quality.status
        quality_report_path = str(quality_report)
    source_snapshot = {
        "schema_version": BACKFILLED_OUTCOME_SNAPSHOT_SCHEMA_VERSION,
        "replay_id": replay_id,
        "replay_root": str(source_dir),
        "replay_manifest_checksum": _sha256_file(source_dir / "historical_replay_manifest.json"),
        "replay_source_snapshot_checksum": _sha256_file(
            source_dir / "historical_replay_source_snapshot.json"
        ),
        "replay_manifest": replay_manifest,
        "replay_events": replay_events,
        "generated_at": generated.isoformat(),
        "config_path": str(config_path),
        "config_checksum": _sha256_file(config_path),
        "config": config,
        "windows": windows,
        "session_source": "validated_price_cache_dates",
        "cost_rate": cost_rate,
        "cost_role": "initial_turnover_deduction_from_return",
        "policy_id": _text(policy_metadata.get("policy_id")),
        "policy_version": _text(policy_metadata.get("version")),
        "prices_path": str(prices_path),
        "prices_checksum": _sha256_file(prices_path),
        "rates_path": str(rates_path),
        "rates_checksum": _sha256_file(rates_path),
        "price_rows": price_rows,
        "data_quality_status": quality_status,
        "data_quality_gate_skipped_for_test": not enforce_data_quality_gate,
        "production_effect": "none",
        "broker_action_taken": False,
    }
    source_snapshot_path = backfill_dir / "backfilled_outcome_source_snapshot.json"
    _write_json(source_snapshot_path, source_snapshot)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backfill_outcome_manifest",
        "backfill_id": backfill_dir.name,
        "replay_id": replay_id,
        "generated_at": generated.isoformat(),
        "status": status,
        "outcome_mode": OUTCOME_MODE_HISTORICAL_REPLAY,
        "tracked_windows": windows,
        "session_source": "validated_price_cache_dates",
        "cost_rate": cost_rate,
        "cost_role": "initial_turnover_deduction_from_return",
        "policy_id": _text(policy_metadata.get("policy_id")),
        "policy_version": _text(policy_metadata.get("version")),
        "replay_event_count": replay_manifest.get("replay_event_count", len(replay_events)),
        "available_count": rollup.get("AVAILABLE", 0),
        "pending_count": rollup.get("PENDING", 0),
        "insufficient_data_count": rollup.get("INSUFFICIENT_DATA", 0),
        "best_variant": summary.get("best_variant", "MISSING"),
        "data_quality_status": quality_status,
        "data_quality_report_path": quality_report_path,
        "prices_path": str(prices_path),
        "source_replay_path": str(source_dir / "historical_replay_manifest.json"),
        "source_replay_validation_status": "PASS",
        "source_snapshot_path": str(source_snapshot_path),
        "source_snapshot_checksum": _sha256_file(source_snapshot_path),
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
    summary = _read_optional_json(backfill_dir / "variant_performance_summary.json") or {}
    snapshot_path = backfill_dir / "backfilled_outcome_source_snapshot.json"
    if not snapshot_path.is_file():
        legacy_checks = _backfill_shallow_checks(
            backfill_id=backfill_id,
            backfill_dir=backfill_dir,
            manifest=manifest,
            rows=rows,
        )
        payload = _validation_payload(
            report_type="etf_dynamic_v3_backfill_outcome_validation",
            artifact_id_key="backfill_id",
            artifact_id=backfill_id,
            checks=legacy_checks,
        )
        if payload["status"] == "PASS":
            payload["status"] = "PASS_WITH_WARNINGS"
        payload["source_snapshot_status"] = "LEGACY_UNSNAPSHOTTED"
        return payload
    snapshot = _read_optional_json(snapshot_path) or {}
    source_root = Path(_text(snapshot.get("replay_root")))
    source_replay_id = _text(snapshot.get("replay_id"))
    source_manifest_path = source_root / "historical_replay_manifest.json"
    source_snapshot_path = source_root / "historical_replay_source_snapshot.json"
    config_path = Path(_text(snapshot.get("config_path")))
    prices_path = Path(_text(snapshot.get("prices_path")))
    rates_path = Path(_text(snapshot.get("rates_path")))
    generated = None
    try:
        frozen_prices = pd.DataFrame(
            _records(snapshot.get("price_rows")),
            columns=["symbol", "date", "adj_close"],
        ).rename(columns={"date": "_date", "adj_close": "_adj_close"})
        frozen_prices["_date"] = pd.to_datetime(frozen_prices["_date"]).dt.date
        generated = _datetime_from_any(snapshot.get("generated_at"))
        if generated is None:
            raise DynamicV3HistoricalReplayError("backfill snapshot generated_at is invalid")
        expected_rows, expected_summary, expected_rollup, expected_status = _backfill_views(
            _records(snapshot.get("replay_events")),
            windows=[_int(value) for value in snapshot.get("windows", [])],
            prices=frozen_prices,
            generated_date=generated.date(),
            cost_rate=_float(snapshot.get("cost_rate")),
        )
        replay_error = ""
    except Exception as exc:  # noqa: BLE001
        expected_rows, expected_summary, expected_rollup, expected_status = [], {}, Counter(), ""
        replay_error = str(exc)
    try:
        source_validation = validate_historical_replay_artifact(
            replay_id=source_replay_id,
            output_dir=source_root.parent,
        )
    except Exception as exc:  # noqa: BLE001
        source_validation = {"status": "FAIL", "error": str(exc)}
    source_files_match = (
        source_manifest_path.is_file()
        and source_snapshot_path.is_file()
        and config_path.is_file()
        and prices_path.is_file()
        and rates_path.is_file()
        and snapshot.get("replay_manifest_checksum") == _sha256_file(source_manifest_path)
        and snapshot.get("replay_source_snapshot_checksum") == _sha256_file(source_snapshot_path)
        and snapshot.get("config_checksum") == _sha256_file(config_path)
        and snapshot.get("prices_checksum") == _sha256_file(prices_path)
        and snapshot.get("rates_checksum") == _sha256_file(rates_path)
    )
    dq_skipped = snapshot.get("data_quality_gate_skipped_for_test") is True
    dq_valid = snapshot.get("data_quality_status") == "SKIPPED_EXPLICIT_TEST_FIXTURE"
    if not dq_skipped and source_files_match:
        quality = _validate_cached_data_quality(
            as_of=generated.date() if generated is not None else date.min,
            prices_path=prices_path,
            rates_path=rates_path,
        )
        dq_valid = quality.passed and quality.status == snapshot.get("data_quality_status")
    try:
        embedded_source_matches = (
            source_manifest_path.is_file()
            and config_path.is_file()
            and snapshot.get("replay_manifest") == _read_json(source_manifest_path)
            and snapshot.get("replay_events") == _read_jsonl(source_root / "replay_events.jsonl")
            and snapshot.get("config") == load_paper_portfolio_config(config_path)
            and snapshot.get("price_rows")
            == _frozen_replay_price_rows(
                _load_prices_for_replay(
                    prices_path,
                    _records(snapshot.get("replay_events")),
                ),
                generated_date=generated.date() if generated is not None else date.min,
            )
        )
    except Exception:  # noqa: BLE001
        embedded_source_matches = False
    snapshot_config = _mapping(snapshot.get("config"))
    snapshot_policy = _mapping(snapshot_config.get("policy_metadata"))
    try:
        snapshot_policy_matches = all(
            (
                snapshot.get("windows") == _configured_outcome_windows(snapshot_config),
                _float(snapshot.get("cost_rate")) == _backfill_cost_rate(snapshot_config),
                snapshot.get("cost_role") == "initial_turnover_deduction_from_return",
                snapshot.get("session_source") == "validated_price_cache_dates",
                snapshot.get("policy_id") == _text(snapshot_policy.get("policy_id")),
                snapshot.get("policy_version") == _text(snapshot_policy.get("version")),
            )
        )
    except Exception:  # noqa: BLE001
        snapshot_policy_matches = False
    expected_report = render_backfill_outcome_report(manifest, expected_summary)
    report_path = backfill_dir / "backfill_outcome_report.md"
    derived_manifest_matches = all(
        (
            manifest.get("replay_id") == source_replay_id,
            manifest.get("generated_at") == snapshot.get("generated_at"),
            manifest.get("tracked_windows") == snapshot.get("windows"),
            manifest.get("session_source") == snapshot.get("session_source"),
            _float(manifest.get("cost_rate")) == _float(snapshot.get("cost_rate")),
            manifest.get("cost_role") == snapshot.get("cost_role"),
            manifest.get("policy_id") == snapshot.get("policy_id"),
            manifest.get("policy_version") == snapshot.get("policy_version"),
            manifest.get("status") == expected_status,
            _int(manifest.get("replay_event_count"))
            == len(_records(snapshot.get("replay_events"))),
            _int(manifest.get("available_count")) == expected_rollup.get("AVAILABLE", 0),
            _int(manifest.get("pending_count")) == expected_rollup.get("PENDING", 0),
            _int(manifest.get("insufficient_data_count"))
            == expected_rollup.get("INSUFFICIENT_DATA", 0),
            manifest.get("best_variant") == expected_summary.get("best_variant", "MISSING"),
            manifest.get("data_quality_status") == snapshot.get("data_quality_status"),
            manifest.get("source_replay_validation_status") == "PASS",
        )
    )
    checks = [
        *_backfill_shallow_checks(
            backfill_id=backfill_id,
            backfill_dir=backfill_dir,
            manifest=manifest,
            rows=rows,
        ),
        _check(
            "source_snapshot_schema_valid",
            snapshot.get("schema_version") == BACKFILLED_OUTCOME_SNAPSHOT_SCHEMA_VERSION,
            BACKFILLED_OUTCOME_SNAPSHOT_SCHEMA_VERSION,
        ),
        _check(
            "source_snapshot_checksum_matches",
            manifest.get("source_snapshot_checksum") == _sha256_file(snapshot_path),
            "backfill source snapshot",
        ),
        _check(
            "source_replay_validation_passes",
            source_validation.get("status") == "PASS",
            source_replay_id,
        ),
        _check("source_files_unchanged", source_files_match, source_replay_id),
        _check(
            "embedded_replay_and_config_match_source",
            embedded_source_matches,
            source_replay_id,
        ),
        _check(
            "snapshot_policy_inputs_recomputed",
            snapshot_policy_matches,
            "windows, cost, sessions, policy metadata",
        ),
        _check("data_quality_evidence_valid", dq_valid, _text(snapshot.get("data_quality_status"))),
        _check("outcome_rows_recomputed", not replay_error and rows == expected_rows, replay_error),
        _check("summary_recomputed", summary == expected_summary, "variant summary"),
        _check("manifest_derived_fields_match", derived_manifest_matches, "manifest"),
        _check(
            "report_recomputed",
            report_path.is_file() and report_path.read_text(encoding="utf-8") == expected_report,
            "Markdown report",
        ),
        _check(
            "non_available_metrics_are_null",
            all(
                row.get(field) is None
                for row in rows
                if row.get("outcome_status") != "AVAILABLE"
                for field in (
                    "gross_return",
                    "estimated_cost",
                    "return",
                    "relative_to_no_trade",
                    "relative_to_consensus_target",
                    "relative_to_limited_adjustment",
                    "max_drawdown",
                    "realized_volatility",
                )
            ),
            "PENDING/INSUFFICIENT metrics must be null",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_backfill_outcome_validation",
        artifact_id_key="backfill_id",
        artifact_id=backfill_id,
        checks=checks,
    )


def _backfill_shallow_checks(
    *,
    backfill_id: str,
    backfill_dir: Path,
    manifest: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        _check("manifest_exists", (backfill_dir / "backfill_manifest.json").is_file(), ""),
        _check(
            "outcome_rows_exists",
            (backfill_dir / "replay_outcome_windows.jsonl").is_file(),
            "",
        ),
        _check("summary_exists", (backfill_dir / "variant_performance_summary.json").is_file(), ""),
        _check("report_exists", (backfill_dir / "backfill_outcome_report.md").is_file(), ""),
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


def run_historical_paper_sim(
    *,
    replay_id: str,
    variant: str = "limited_adjustment",
    replay_dir: Path = DEFAULT_HISTORICAL_REPLAY_DIR,
    output_dir: Path = DEFAULT_HISTORICAL_PAPER_SIM_DIR,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    config_path: Path = DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    enforce_data_quality_gate: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    if variant not in SIM_VARIANTS:
        raise DynamicV3HistoricalReplayError(f"unsupported simulation variant: {variant}")
    generated = _require_aware_utc(generated_at or datetime.now(UTC), "generated_at")
    source_dir = replay_dir / replay_id
    source_validation = validate_historical_replay_artifact(
        replay_id=replay_id,
        output_dir=replay_dir,
    )
    if source_validation.get("status") != "PASS":
        raise DynamicV3HistoricalReplayError(
            "historical paper sim requires a fully validated snapshotted replay"
        )
    replay_manifest = _read_json(source_dir / "historical_replay_manifest.json")
    replay_generated = _datetime_from_any(replay_manifest.get("generated_at"))
    if replay_generated is None or generated < replay_generated:
        raise DynamicV3HistoricalReplayError(
            "historical paper sim generated_at cannot precede replay generated_at"
        )
    events = sorted(_read_jsonl(source_dir / "replay_events.jsonl"), key=lambda row: row["as_of"])
    config = load_paper_portfolio_config(config_path)
    cost_rate = _backfill_cost_rate(config)
    policy_metadata = _mapping(config.get("policy_metadata"))
    quality = None
    if enforce_data_quality_gate:
        quality = _validate_cached_data_quality(
            as_of=generated.date(),
            prices_path=prices_path,
            rates_path=rates_path,
        )
        if not quality.passed:
            raise DynamicV3HistoricalReplayError(
                f"historical paper sim data quality gate failed: {quality.status}"
            )
    prices = _load_prices_for_replay(prices_path, events)
    price_rows = _frozen_replay_price_rows(prices, generated_date=generated.date())
    frozen_prices = pd.DataFrame(
        price_rows,
        columns=["symbol", "date", "adj_close"],
    ).rename(columns={"date": "_date", "adj_close": "_adj_close"})
    frozen_prices["_date"] = pd.to_datetime(frozen_prices["_date"]).dt.date
    history, ledger, summary = _simulate_paper_history(
        events,
        variant=variant,
        prices=frozen_prices,
        cost_rate=cost_rate,
    )
    sim_id = _stable_id("historical-paper-sim", replay_id, variant, generated.isoformat())
    sim_dir = _unique_dir(output_dir / sim_id)
    sim_dir.mkdir(parents=True, exist_ok=False)
    quality_status = "SKIPPED_EXPLICIT_TEST_FIXTURE"
    quality_report_path = ""
    if enforce_data_quality_gate:
        assert quality is not None
        quality_report = sim_dir / "validate_data_quality_report.md"
        write_data_quality_report(quality, quality_report)
        quality_status = quality.status
        quality_report_path = str(quality_report)
    source_snapshot = {
        "schema_version": HISTORICAL_PAPER_SIM_SNAPSHOT_SCHEMA_VERSION,
        "replay_id": replay_id,
        "replay_root": str(source_dir),
        "replay_manifest_checksum": _sha256_file(source_dir / "historical_replay_manifest.json"),
        "replay_source_snapshot_checksum": _sha256_file(
            source_dir / "historical_replay_source_snapshot.json"
        ),
        "replay_manifest": replay_manifest,
        "replay_events": events,
        "variant": variant,
        "generated_at": generated.isoformat(),
        "config_path": str(config_path),
        "config_checksum": _sha256_file(config_path),
        "config": config,
        "policy_id": _text(policy_metadata.get("policy_id")),
        "policy_version": _text(policy_metadata.get("version")),
        "cost_rate": cost_rate,
        "cost_role": "event_target_reset_cost_on_pre_trade_equity",
        "prices_path": str(prices_path),
        "prices_checksum": _sha256_file(prices_path),
        "rates_path": str(rates_path),
        "rates_checksum": _sha256_file(rates_path),
        "price_rows": price_rows,
        "data_quality_status": quality_status,
        "data_quality_gate_skipped_for_test": not enforce_data_quality_gate,
        "production_effect": "none",
        "broker_action_taken": False,
    }
    source_snapshot_path = sim_dir / "historical_paper_sim_source_snapshot.json"
    _write_json(source_snapshot_path, source_snapshot)
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
        "source_replay_validation_status": "PASS",
        "data_quality_status": quality_status,
        "data_quality_report_path": quality_report_path,
        "policy_id": _text(policy_metadata.get("policy_id")),
        "policy_version": _text(policy_metadata.get("version")),
        "cost_rate": cost_rate,
        "cost_role": "event_target_reset_cost_on_pre_trade_equity",
        "source_replay_path": str(source_dir / "historical_replay_manifest.json"),
        "source_snapshot_path": str(source_snapshot_path),
        "source_snapshot_checksum": _sha256_file(source_snapshot_path),
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
    summary = _read_optional_json(sim_dir / "simulated_performance_summary.json") or {}
    snapshot_path = sim_dir / "historical_paper_sim_source_snapshot.json"
    shallow_checks = [
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
    if not snapshot_path.is_file():
        payload = _validation_payload(
            report_type="etf_dynamic_v3_historical_paper_sim_validation",
            artifact_id_key="sim_id",
            artifact_id=sim_id,
            checks=shallow_checks,
        )
        if payload["status"] == "PASS":
            payload["status"] = "PASS_WITH_WARNINGS"
        payload["source_snapshot_status"] = "LEGACY_UNSNAPSHOTTED"
        return payload
    snapshot = _read_optional_json(snapshot_path) or {}
    source_root = Path(_text(snapshot.get("replay_root")))
    source_replay_id = _text(snapshot.get("replay_id"))
    source_manifest_path = source_root / "historical_replay_manifest.json"
    source_replay_snapshot_path = source_root / "historical_replay_source_snapshot.json"
    config_path = Path(_text(snapshot.get("config_path")))
    prices_path = Path(_text(snapshot.get("prices_path")))
    rates_path = Path(_text(snapshot.get("rates_path")))
    generated = _datetime_from_any(snapshot.get("generated_at"))
    try:
        source_validation = validate_historical_replay_artifact(
            replay_id=source_replay_id,
            output_dir=source_root.parent,
        )
    except Exception as exc:  # noqa: BLE001
        source_validation = {"status": "FAIL", "error": str(exc)}
    source_files_match = (
        source_manifest_path.is_file()
        and source_replay_snapshot_path.is_file()
        and config_path.is_file()
        and prices_path.is_file()
        and rates_path.is_file()
        and snapshot.get("replay_manifest_checksum") == _sha256_file(source_manifest_path)
        and snapshot.get("replay_source_snapshot_checksum")
        == _sha256_file(source_replay_snapshot_path)
        and snapshot.get("config_checksum") == _sha256_file(config_path)
        and snapshot.get("prices_checksum") == _sha256_file(prices_path)
        and snapshot.get("rates_checksum") == _sha256_file(rates_path)
    )
    try:
        frozen_prices = pd.DataFrame(
            _records(snapshot.get("price_rows")),
            columns=["symbol", "date", "adj_close"],
        ).rename(columns={"date": "_date", "adj_close": "_adj_close"})
        frozen_prices["_date"] = pd.to_datetime(frozen_prices["_date"]).dt.date
        expected_history, expected_ledger, expected_summary = _simulate_paper_history(
            _records(snapshot.get("replay_events")),
            variant=_text(snapshot.get("variant")),
            prices=frozen_prices,
            cost_rate=_float(snapshot.get("cost_rate")),
        )
        replay_error = ""
    except Exception as exc:  # noqa: BLE001
        expected_history, expected_ledger, expected_summary = [], [], {}
        replay_error = str(exc)
    try:
        embedded_source_matches = (
            generated is not None
            and snapshot.get("replay_manifest") == _read_json(source_manifest_path)
            and snapshot.get("replay_events") == _read_jsonl(source_root / "replay_events.jsonl")
            and snapshot.get("config") == load_paper_portfolio_config(config_path)
            and snapshot.get("price_rows")
            == _frozen_replay_price_rows(
                _load_prices_for_replay(
                    prices_path,
                    _records(snapshot.get("replay_events")),
                ),
                generated_date=generated.date(),
            )
        )
    except Exception:  # noqa: BLE001
        embedded_source_matches = False
    snapshot_config = _mapping(snapshot.get("config"))
    snapshot_policy = _mapping(snapshot_config.get("policy_metadata"))
    try:
        policy_matches = all(
            (
                _float(snapshot.get("cost_rate")) == _backfill_cost_rate(snapshot_config),
                snapshot.get("cost_role") == "event_target_reset_cost_on_pre_trade_equity",
                snapshot.get("policy_id") == _text(snapshot_policy.get("policy_id")),
                snapshot.get("policy_version") == _text(snapshot_policy.get("version")),
            )
        )
    except Exception:  # noqa: BLE001
        policy_matches = False
    dq_skipped = snapshot.get("data_quality_gate_skipped_for_test") is True
    dq_valid = snapshot.get("data_quality_status") == "SKIPPED_EXPLICIT_TEST_FIXTURE"
    if not dq_skipped and source_files_match and generated is not None:
        quality = _validate_cached_data_quality(
            as_of=generated.date(),
            prices_path=prices_path,
            rates_path=rates_path,
        )
        dq_valid = quality.passed and quality.status == snapshot.get("data_quality_status")
    expected_report = render_historical_paper_sim_report(manifest, expected_summary)
    report_path = sim_dir / "historical_paper_sim_report.md"
    derived_manifest_matches = all(
        (
            manifest.get("replay_id") == source_replay_id,
            manifest.get("generated_at") == snapshot.get("generated_at"),
            manifest.get("variant") == snapshot.get("variant"),
            manifest.get("status") == expected_summary.get("simulation_status"),
            manifest.get("source_replay_validation_status") == "PASS",
            manifest.get("data_quality_status") == snapshot.get("data_quality_status"),
            manifest.get("policy_id") == snapshot.get("policy_id"),
            manifest.get("policy_version") == snapshot.get("policy_version"),
            _float(manifest.get("cost_rate")) == _float(snapshot.get("cost_rate")),
            manifest.get("cost_role") == snapshot.get("cost_role"),
        )
    )
    checks = [
        *shallow_checks,
        _check(
            "source_snapshot_schema_valid",
            snapshot.get("schema_version") == HISTORICAL_PAPER_SIM_SNAPSHOT_SCHEMA_VERSION,
            HISTORICAL_PAPER_SIM_SNAPSHOT_SCHEMA_VERSION,
        ),
        _check(
            "source_snapshot_checksum_matches",
            manifest.get("source_snapshot_checksum") == _sha256_file(snapshot_path),
            "historical paper sim source snapshot",
        ),
        _check(
            "source_replay_validation_passes",
            source_validation.get("status") == "PASS",
            source_replay_id,
        ),
        _check("source_files_unchanged", source_files_match, source_replay_id),
        _check("embedded_sources_match", embedded_source_matches, source_replay_id),
        _check("snapshot_policy_recomputed", policy_matches, "cost and policy"),
        _check("data_quality_evidence_valid", dq_valid, _text(snapshot.get("data_quality_status"))),
        _check(
            "state_history_recomputed",
            not replay_error and history == expected_history,
            replay_error or "state history",
        ),
        _check("trade_ledger_recomputed", ledger == expected_ledger, "trade ledger"),
        _check("performance_summary_recomputed", summary == expected_summary, "summary"),
        _check("manifest_derived_fields_match", derived_manifest_matches, "manifest"),
        _check(
            "report_recomputed",
            report_path.is_file() and report_path.read_text(encoding="utf-8") == expected_report,
            "Markdown report",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_historical_paper_sim_validation",
        artifact_id_key="sim_id",
        artifact_id=sim_id,
        checks=checks,
    )


def _load_replay_performance_review_policy(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise DynamicV3HistoricalReplayError(f"review policy is missing: {path}")
    loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(loaded, dict):
        raise DynamicV3HistoricalReplayError("review policy must be a mapping")
    metadata = _mapping(loaded.get("policy_metadata"))
    gate = _mapping(loaded.get("evidence_gate"))
    safety = _mapping(loaded.get("safety"))
    required_metadata = (
        "policy_id",
        "owner",
        "version",
        "status",
        "rationale",
        "intended_effect",
        "review_condition",
    )
    if any(not _text(metadata.get(field)) for field in required_metadata):
        raise DynamicV3HistoricalReplayError("review policy metadata is incomplete")
    if metadata.get("status") != "pilot_baseline":
        raise DynamicV3HistoricalReplayError("review policy status must be pilot_baseline")
    window = _int(gate.get("comparison_window_trading_days"))
    minimum_events = _int(gate.get("minimum_distinct_replay_event_count"))
    minimum_windows = _int(gate.get("minimum_available_variant_window_count"))
    boundary = gate.get("positive_relative_return_boundary")
    if window not in OUTCOME_WINDOWS or minimum_events <= 0 or minimum_windows <= 0:
        raise DynamicV3HistoricalReplayError("review evidence gate is invalid")
    if isinstance(boundary, bool) or not isinstance(boundary, (int, float)):
        raise DynamicV3HistoricalReplayError("positive relative return boundary is invalid")
    if not math.isfinite(float(boundary)):
        raise DynamicV3HistoricalReplayError("positive relative return boundary must be finite")
    if (
        not all(
            safety.get(field) is expected
            for field, expected in (
                ("manual_review_required", True),
                ("requires_owner_approval", True),
                ("automatic_config_update", False),
                ("automatic_candidate_promotion", False),
                ("broker_action_taken", False),
            )
        )
        or safety.get("production_effect") != "none"
    ):
        raise DynamicV3HistoricalReplayError("review policy safety boundary is invalid")
    return loaded


def _review_source_bundle(root: Path, names: Sequence[str]) -> dict[str, Any]:
    bundle: dict[str, Any] = {}
    for name in names:
        path = root / name
        if not path.is_file():
            raise DynamicV3HistoricalReplayError(f"review source is missing: {path}")
        if name.endswith(".jsonl"):
            content: Any = _read_jsonl(path)
        elif name.endswith(".json"):
            content = _read_json(path)
        else:
            content = path.read_text(encoding="utf-8")
        bundle[name] = {
            "path": str(path),
            "checksum": _sha256_file(path),
            "content": content,
        }
    return bundle


def _review_bundle_matches(bundle: Mapping[str, Any]) -> bool:
    try:
        for name, raw in bundle.items():
            entry = _mapping(raw)
            path = Path(_text(entry.get("path")))
            if not path.is_file() or entry.get("checksum") != _sha256_file(path):
                return False
            if name.endswith(".jsonl"):
                live: Any = _read_jsonl(path)
            elif name.endswith(".json"):
                live = _read_json(path)
            else:
                live = path.read_text(encoding="utf-8")
            if live != entry.get("content"):
                return False
    except Exception:  # noqa: BLE001
        return False
    return True


def _review_bundle_content(bundle: Mapping[str, Any], name: str) -> Any:
    return _mapping(bundle.get(name)).get("content")


def run_replay_performance_review(
    *,
    backfill_id: str,
    sim_id: str,
    backfill_dir: Path = DEFAULT_BACKFILLED_OUTCOME_DIR,
    sim_dir: Path = DEFAULT_HISTORICAL_PAPER_SIM_DIR,
    output_dir: Path = DEFAULT_REPLAY_PERFORMANCE_REVIEW_DIR,
    policy_path: Path = DEFAULT_REPLAY_PERFORMANCE_REVIEW_POLICY_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _require_aware_utc(generated_at or datetime.now(UTC), "generated_at")
    source_backfill_dir = backfill_dir / backfill_id
    source_sim_dir = sim_dir / sim_id
    backfill_validation = validate_backfill_outcome_artifact(
        backfill_id=backfill_id,
        output_dir=backfill_dir,
    )
    if backfill_validation.get("status") != "PASS":
        raise DynamicV3HistoricalReplayError(
            f"source backfill validation must PASS: {backfill_validation.get('status')}"
        )
    sim_validation = validate_historical_paper_sim_artifact(
        sim_id=sim_id,
        output_dir=sim_dir,
    )
    if sim_validation.get("status") != "PASS":
        raise DynamicV3HistoricalReplayError(
            f"source historical paper simulation validation must PASS: "
            f"{sim_validation.get('status')}"
        )
    backfill_manifest = _read_json(source_backfill_dir / "backfill_manifest.json")
    outcome_rows = _read_jsonl(source_backfill_dir / "replay_outcome_windows.jsonl")
    variant_summary = _read_json(source_backfill_dir / "variant_performance_summary.json")
    sim_manifest = _read_json(source_sim_dir / "historical_paper_sim_manifest.json")
    sim_summary = _read_json(source_sim_dir / "simulated_performance_summary.json")
    replay_id = _text(backfill_manifest.get("replay_id"))
    if not replay_id or replay_id != _text(sim_manifest.get("replay_id")):
        raise DynamicV3HistoricalReplayError("backfill and simulation must share one replay_id")
    source_times = [
        _datetime_from_any(backfill_manifest.get("generated_at")),
        _datetime_from_any(sim_manifest.get("generated_at")),
    ]
    if any(value is None for value in source_times):
        raise DynamicV3HistoricalReplayError("source generated_at must be timezone-aware")
    if any(generated < value for value in source_times if value is not None):
        raise DynamicV3HistoricalReplayError(
            "review generated_at must not precede source artifacts"
        )
    policy = _load_replay_performance_review_policy(policy_path)
    backfill_bundle = _review_source_bundle(
        source_backfill_dir,
        (
            "backfilled_outcome_source_snapshot.json",
            "backfill_manifest.json",
            "replay_outcome_windows.jsonl",
            "variant_performance_summary.json",
            "backfill_outcome_report.md",
        ),
    )
    sim_bundle = _review_source_bundle(
        source_sim_dir,
        (
            "historical_paper_sim_source_snapshot.json",
            "historical_paper_sim_manifest.json",
            "simulated_paper_state_history.jsonl",
            "simulated_trade_ledger.jsonl",
            "simulated_performance_summary.json",
            "historical_paper_sim_report.md",
        ),
    )
    effectiveness = _advisory_rule_effectiveness(outcome_rows, sim_summary, policy)
    recommendations = _calibration_recommendations(
        outcome_rows,
        variant_summary,
        sim_summary,
        policy,
    )
    review_id = _stable_id("replay-performance-review", backfill_id, sim_id, generated.isoformat())
    review_dir = _unique_dir(output_dir / review_id)
    available_outcome_count = _int(backfill_manifest.get("available_count"))
    status = (
        "AVAILABLE"
        if available_outcome_count
        else _text(backfill_manifest.get("status"), "INSUFFICIENT_DATA")
    )
    policy_metadata = _mapping(policy.get("policy_metadata"))
    primary = _records(recommendations.get("recommendations"))[0]
    source_snapshot = {
        "schema_version": REPLAY_PERFORMANCE_REVIEW_SNAPSHOT_SCHEMA_VERSION,
        "review_id": review_dir.name,
        "generated_at": generated.isoformat(),
        "replay_id": replay_id,
        "backfill_id": backfill_id,
        "sim_id": sim_id,
        "backfill_root": str(source_backfill_dir),
        "sim_root": str(source_sim_dir),
        "backfill_files": backfill_bundle,
        "sim_files": sim_bundle,
        "backfill_validation_status": "PASS",
        "sim_validation_status": "PASS",
        "policy_path": str(policy_path),
        "policy_checksum": _sha256_file(policy_path),
        "policy": policy,
        "policy_id": policy_metadata.get("policy_id"),
        "policy_version": policy_metadata.get("version"),
        "source_role": "historical_replay_evaluation_only",
    }
    review_dir.mkdir(parents=True, exist_ok=False)
    source_snapshot_path = review_dir / "replay_performance_review_source_snapshot.json"
    _write_json(source_snapshot_path, source_snapshot)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_replay_performance_manifest",
        "review_id": review_dir.name,
        "backfill_id": backfill_id,
        "sim_id": sim_id,
        "replay_id": replay_id,
        "generated_at": generated.isoformat(),
        "status": status,
        "outcome_mode": OUTCOME_MODE_HISTORICAL_REPLAY,
        "replay_event_count": backfill_manifest.get("replay_event_count", 0),
        "available_outcome_count": available_outcome_count,
        "best_variant": variant_summary.get("best_variant", "MISSING"),
        "limited_adjustment_vs_no_trade": variant_summary.get("limited_adjustment_vs_no_trade_5d"),
        "calibration_recommendation": primary["type"],
        "next_action": primary["type"],
        "directional_evidence_ready": recommendations["directional_evidence_ready"],
        "distinct_replay_event_count": recommendations["distinct_replay_event_count"],
        "available_comparison_window_count": recommendations["available_comparison_window_count"],
        "policy_id": policy_metadata.get("policy_id"),
        "policy_version": policy_metadata.get("version"),
        "source_snapshot_path": str(source_snapshot_path),
        "source_snapshot_checksum": _sha256_file(source_snapshot_path),
        "source_backfill_validation_status": "PASS",
        "source_sim_validation_status": "PASS",
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
        "source_snapshot": source_snapshot,
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
    effectiveness = _read_optional_json(review_dir / "advisory_rule_effectiveness.json") or {}
    recommendations = _read_optional_json(review_dir / "calibration_recommendations.json") or {}
    shallow_checks = [
        _check(
            "manifest_exists",
            (review_dir / "replay_performance_manifest.json").is_file(),
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
    snapshot_path = review_dir / "replay_performance_review_source_snapshot.json"
    if not snapshot_path.is_file():
        payload = _validation_payload(
            report_type="etf_dynamic_v3_replay_performance_review_validation",
            artifact_id_key="review_id",
            artifact_id=review_id,
            checks=shallow_checks,
        )
        if payload["status"] == "PASS":
            payload["status"] = "PASS_WITH_WARNINGS"
        payload["source_snapshot_status"] = "LEGACY_UNSNAPSHOTTED"
        return payload
    snapshot = _read_optional_json(snapshot_path) or {}
    backfill_bundle = _mapping(snapshot.get("backfill_files"))
    sim_bundle = _mapping(snapshot.get("sim_files"))
    policy_path = Path(_text(snapshot.get("policy_path")))
    try:
        policy = _load_replay_performance_review_policy(policy_path)
        policy_valid = (
            snapshot.get("policy_checksum") == _sha256_file(policy_path)
            and snapshot.get("policy") == policy
        )
    except Exception:  # noqa: BLE001
        policy = {}
        policy_valid = False
    try:
        backfill_root = Path(_text(snapshot.get("backfill_root")))
        sim_root = Path(_text(snapshot.get("sim_root")))
        backfill_validation = validate_backfill_outcome_artifact(
            backfill_id=_text(snapshot.get("backfill_id")),
            output_dir=backfill_root.parent,
        )
        sim_validation = validate_historical_paper_sim_artifact(
            sim_id=_text(snapshot.get("sim_id")),
            output_dir=sim_root.parent,
        )
    except Exception as exc:  # noqa: BLE001
        backfill_validation = {"status": "FAIL", "error": str(exc)}
        sim_validation = {"status": "FAIL", "error": str(exc)}
    try:
        backfill_manifest = _mapping(
            _review_bundle_content(backfill_bundle, "backfill_manifest.json")
        )
        outcome_rows = _records(
            _review_bundle_content(backfill_bundle, "replay_outcome_windows.jsonl")
        )
        variant_summary = _mapping(
            _review_bundle_content(backfill_bundle, "variant_performance_summary.json")
        )
        sim_manifest = _mapping(
            _review_bundle_content(sim_bundle, "historical_paper_sim_manifest.json")
        )
        sim_summary = _mapping(
            _review_bundle_content(sim_bundle, "simulated_performance_summary.json")
        )
        expected_effectiveness = _advisory_rule_effectiveness(outcome_rows, sim_summary, policy)
        expected_recommendations = _calibration_recommendations(
            outcome_rows,
            variant_summary,
            sim_summary,
            policy,
        )
        available_count = _int(backfill_manifest.get("available_count"))
        expected_status = (
            "AVAILABLE"
            if available_count
            else _text(backfill_manifest.get("status"), "INSUFFICIENT_DATA")
        )
        primary = _records(expected_recommendations.get("recommendations"))[0]
        metadata = _mapping(policy.get("policy_metadata"))
        expected_manifest_fields = {
            "review_id": review_id,
            "backfill_id": snapshot.get("backfill_id"),
            "sim_id": snapshot.get("sim_id"),
            "replay_id": snapshot.get("replay_id"),
            "generated_at": snapshot.get("generated_at"),
            "status": expected_status,
            "replay_event_count": backfill_manifest.get("replay_event_count", 0),
            "available_outcome_count": available_count,
            "best_variant": variant_summary.get("best_variant", "MISSING"),
            "limited_adjustment_vs_no_trade": variant_summary.get(
                "limited_adjustment_vs_no_trade_5d"
            ),
            "calibration_recommendation": primary.get("type"),
            "next_action": primary.get("type"),
            "directional_evidence_ready": expected_recommendations.get(
                "directional_evidence_ready"
            ),
            "distinct_replay_event_count": expected_recommendations.get(
                "distinct_replay_event_count"
            ),
            "available_comparison_window_count": expected_recommendations.get(
                "available_comparison_window_count"
            ),
            "policy_id": metadata.get("policy_id"),
            "policy_version": metadata.get("version"),
            "source_backfill_validation_status": "PASS",
            "source_sim_validation_status": "PASS",
        }
        lineage_valid = backfill_manifest.get("replay_id") == snapshot.get(
            "replay_id"
        ) and sim_manifest.get("replay_id") == snapshot.get("replay_id")
        recompute_error = ""
    except Exception as exc:  # noqa: BLE001
        expected_effectiveness = {}
        expected_recommendations = {}
        expected_manifest_fields = {}
        sim_summary = {}
        lineage_valid = False
        recompute_error = str(exc)
    expected_report = render_replay_performance_review(
        manifest,
        expected_effectiveness,
        expected_recommendations,
        sim_summary,
    )
    expected_reader = render_replay_performance_reader_brief(
        manifest,
        expected_recommendations,
    )
    checks = [
        *shallow_checks,
        _check(
            "source_snapshot_schema_valid",
            snapshot.get("schema_version") == REPLAY_PERFORMANCE_REVIEW_SNAPSHOT_SCHEMA_VERSION,
            REPLAY_PERFORMANCE_REVIEW_SNAPSHOT_SCHEMA_VERSION,
        ),
        _check(
            "source_snapshot_checksum_matches",
            manifest.get("source_snapshot_checksum") == _sha256_file(snapshot_path),
            "review source snapshot",
        ),
        _check(
            "backfill_source_validation_passes", backfill_validation.get("status") == "PASS", ""
        ),
        _check("simulation_source_validation_passes", sim_validation.get("status") == "PASS", ""),
        _check("source_lineage_matches", lineage_valid, _text(snapshot.get("replay_id"))),
        _check(
            "source_files_unchanged",
            _review_bundle_matches(backfill_bundle) and _review_bundle_matches(sim_bundle),
            "backfill and simulation bundles",
        ),
        _check("review_policy_valid_and_unchanged", policy_valid, _text(snapshot.get("policy_id"))),
        _check(
            "effectiveness_recomputed",
            not recompute_error and effectiveness == expected_effectiveness,
            recompute_error,
        ),
        _check(
            "recommendations_recomputed",
            not recompute_error and recommendations == expected_recommendations,
            recompute_error,
        ),
        _check(
            "manifest_derived_fields_match",
            all(manifest.get(key) == value for key, value in expected_manifest_fields.items()),
            "manifest",
        ),
        _check(
            "unsupported_classification_metrics_are_null",
            all(
                row.get("false_alarm_rate") is None and row.get("missed_opportunity_rate") is None
                for row in _records(effectiveness.get("recommendation_effectiveness"))
            ),
            "labels are not identified from relative return alone",
        ),
        _check(
            "report_recomputed",
            (review_dir / "replay_performance_review.md").is_file()
            and (review_dir / "replay_performance_review.md").read_text(encoding="utf-8")
            == expected_report,
            "Markdown report",
        ),
        _check(
            "reader_brief_recomputed",
            (review_dir / "reader_brief_section.md").is_file()
            and (review_dir / "reader_brief_section.md").read_text(encoding="utf-8")
            == expected_reader,
            "Reader Brief section",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_replay_performance_review_validation",
        artifact_id_key="review_id",
        artifact_id=review_id,
        checks=checks,
    )


def _diagnosis_source_bundles(
    *,
    inventory_root: Path,
    replay_root: Path,
    backfill_root: Path,
    sim_root: Path,
    review_root: Path,
) -> dict[str, Any]:
    return {
        "inventory": _review_source_bundle(
            inventory_root,
            (
                "replay_inventory_source_snapshot.json",
                "replay_inventory_manifest.json",
                "replay_artifact_inventory.jsonl",
                "pit_safety_audit.json",
                "replay_coverage_summary.json",
                "replay_inventory_report.md",
            ),
        ),
        "replay": _review_source_bundle(
            replay_root,
            (
                "historical_replay_source_snapshot.json",
                "historical_replay_manifest.json",
                "replay_events.jsonl",
                "replay_decision_inputs.jsonl",
                "replay_action_summary.json",
                "historical_replay_report.md",
            ),
        ),
        "backfill": _review_source_bundle(
            backfill_root,
            (
                "backfilled_outcome_source_snapshot.json",
                "backfill_manifest.json",
                "replay_outcome_windows.jsonl",
                "variant_performance_summary.json",
                "backfill_outcome_report.md",
            ),
        ),
        "sim": _review_source_bundle(
            sim_root,
            (
                "historical_paper_sim_source_snapshot.json",
                "historical_paper_sim_manifest.json",
                "simulated_paper_state_history.jsonl",
                "simulated_trade_ledger.jsonl",
                "simulated_performance_summary.json",
                "historical_paper_sim_report.md",
            ),
        ),
        "review": _review_source_bundle(
            review_root,
            (
                "replay_performance_review_source_snapshot.json",
                "replay_performance_manifest.json",
                "advisory_rule_effectiveness.json",
                "calibration_recommendations.json",
                "replay_performance_review.md",
                "reader_brief_section.md",
            ),
        ),
    }


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
    generated = _require_aware_utc(generated_at or datetime.now(UTC), "generated_at")
    source_inventory_dir = inventory_dir / inventory_id
    source_replay_dir = replay_dir / replay_id
    source_backfill_dir = backfill_dir / backfill_id
    source_sim_dir = sim_dir / sim_id
    source_review_dir = review_dir / review_id

    validations = {
        "inventory": validate_replay_inventory_artifact(
            inventory_id=inventory_id,
            output_dir=inventory_dir,
        ),
        "replay": validate_historical_replay_artifact(
            replay_id=replay_id,
            output_dir=replay_dir,
        ),
        "backfill": validate_backfill_outcome_artifact(
            backfill_id=backfill_id,
            output_dir=backfill_dir,
        ),
        "sim": validate_historical_paper_sim_artifact(sim_id=sim_id, output_dir=sim_dir),
        "review": validate_replay_performance_review_artifact(
            review_id=review_id,
            output_dir=review_dir,
        ),
    }
    failed = [name for name, payload in validations.items() if payload.get("status") != "PASS"]
    if failed:
        raise DynamicV3HistoricalReplayError(
            f"diagnosis source validation must PASS: {', '.join(failed)}"
        )

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
    if not all(
        (
            replay_manifest.get("inventory_id") == inventory_id,
            backfill_manifest.get("replay_id") == replay_id,
            sim_manifest.get("replay_id") == replay_id,
            review_manifest.get("replay_id") == replay_id,
            review_manifest.get("backfill_id") == backfill_id,
            review_manifest.get("sim_id") == sim_id,
        )
    ):
        raise DynamicV3HistoricalReplayError("diagnosis source lineage is inconsistent")
    source_manifests = (
        inventory_manifest,
        replay_manifest,
        backfill_manifest,
        sim_manifest,
        review_manifest,
    )
    source_times = [_datetime_from_any(item.get("generated_at")) for item in source_manifests]
    if any(value is None for value in source_times):
        raise DynamicV3HistoricalReplayError("diagnosis source generated_at is invalid")
    if any(generated < value for value in source_times if value is not None):
        raise DynamicV3HistoricalReplayError("diagnosis generated_at must not precede sources")
    source_bundles = _diagnosis_source_bundles(
        inventory_root=source_inventory_dir,
        replay_root=source_replay_dir,
        backfill_root=source_backfill_dir,
        sim_root=source_sim_dir,
        review_root=source_review_dir,
    )

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
            validation_status="PASS",
            source_snapshot_schema=REPLAY_INVENTORY_SNAPSHOT_SCHEMA_VERSION,
        ),
        _artifact_health_row(
            "historical_replay",
            replay_id,
            source_replay_dir / "historical_replay_manifest.json",
            replay_manifest,
            len(replay_events),
            validation_status="PASS",
            source_snapshot_schema=HISTORICAL_REPLAY_SNAPSHOT_SCHEMA_VERSION,
        ),
        _artifact_health_row(
            "backfilled_outcome",
            backfill_id,
            source_backfill_dir / "backfill_manifest.json",
            backfill_manifest,
            len(outcome_rows),
            validation_status="PASS",
            source_snapshot_schema=BACKFILLED_OUTCOME_SNAPSHOT_SCHEMA_VERSION,
        ),
        _artifact_health_row(
            "historical_paper_sim",
            sim_id,
            source_sim_dir / "historical_paper_sim_manifest.json",
            sim_manifest,
            len(sim_state_history),
            validation_status="PASS",
            source_snapshot_schema=HISTORICAL_PAPER_SIM_SNAPSHOT_SCHEMA_VERSION,
        ),
        _artifact_health_row(
            "replay_performance_review",
            review_id,
            source_review_dir / "replay_performance_manifest.json",
            review_manifest,
            len(_records(calibration.get("recommendations"))),
            validation_status="PASS",
            source_snapshot_schema=REPLAY_PERFORMANCE_REVIEW_SNAPSHOT_SCHEMA_VERSION,
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
        "can_enter_variant_comparison": review_manifest.get("directional_evidence_ready") is True,
        "variant_comparison_evidence_status": (
            "READY_FOR_DIRECTIONAL_REVIEW"
            if review_manifest.get("directional_evidence_ready") is True
            else "INSUFFICIENT_DIRECTIONAL_EVIDENCE"
        ),
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
    source_snapshot = {
        "schema_version": REPLAY_DIAGNOSIS_SNAPSHOT_SCHEMA_VERSION,
        "diagnosis_id": diagnosis_dir.name,
        "generated_at": generated.isoformat(),
        "inventory_id": inventory_id,
        "replay_id": replay_id,
        "backfill_id": backfill_id,
        "sim_id": sim_id,
        "review_id": review_id,
        "source_roots": {
            "inventory": str(source_inventory_dir),
            "replay": str(source_replay_dir),
            "backfill": str(source_backfill_dir),
            "sim": str(source_sim_dir),
            "review": str(source_review_dir),
        },
        "source_validation_statuses": {name: "PASS" for name in validations},
        "source_bundles": source_bundles,
    }
    source_snapshot_path = diagnosis_dir / "replay_diagnosis_source_snapshot.json"
    _write_json(source_snapshot_path, source_snapshot)
    manifest["source_snapshot_path"] = str(source_snapshot_path)
    manifest["source_snapshot_checksum"] = _sha256_file(source_snapshot_path)
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
        "source_snapshot": source_snapshot,
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
    coverage = _read_optional_json(diagnosis_dir / "replay_coverage_breakdown.json") or {}
    health = _read_jsonl(diagnosis_dir / "replay_artifact_health_matrix.jsonl")
    valid_reasons = set(PENDING_REASON_ACTIONS)
    shallow_checks = [
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
            all(
                row.get("exists") is True
                and row.get("artifact_id")
                and row.get("validation_status") in {None, "PASS"}
                for row in health
            ),
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
    snapshot_path = diagnosis_dir / "replay_diagnosis_source_snapshot.json"
    if not snapshot_path.is_file():
        payload = _validation_payload(
            report_type="etf_dynamic_v3_replay_diagnosis_validation",
            artifact_id_key="diagnosis_id",
            artifact_id=diagnosis_id,
            checks=shallow_checks,
        )
        if payload["status"] == "PASS":
            payload["status"] = "PASS_WITH_WARNINGS"
        payload["source_snapshot_status"] = "LEGACY_UNSNAPSHOTTED"
        return payload
    snapshot = _read_optional_json(snapshot_path) or {}
    bundles = _mapping(snapshot.get("source_bundles"))
    roots = _mapping(snapshot.get("source_roots"))
    ids = {
        "inventory": _text(snapshot.get("inventory_id")),
        "replay": _text(snapshot.get("replay_id")),
        "backfill": _text(snapshot.get("backfill_id")),
        "sim": _text(snapshot.get("sim_id")),
        "review": _text(snapshot.get("review_id")),
    }
    try:
        source_validations = {
            "inventory": validate_replay_inventory_artifact(
                inventory_id=ids["inventory"],
                output_dir=Path(_text(roots.get("inventory"))).parent,
            ),
            "replay": validate_historical_replay_artifact(
                replay_id=ids["replay"],
                output_dir=Path(_text(roots.get("replay"))).parent,
            ),
            "backfill": validate_backfill_outcome_artifact(
                backfill_id=ids["backfill"],
                output_dir=Path(_text(roots.get("backfill"))).parent,
            ),
            "sim": validate_historical_paper_sim_artifact(
                sim_id=ids["sim"],
                output_dir=Path(_text(roots.get("sim"))).parent,
            ),
            "review": validate_replay_performance_review_artifact(
                review_id=ids["review"],
                output_dir=Path(_text(roots.get("review"))).parent,
            ),
        }
    except Exception as exc:  # noqa: BLE001
        source_validations = {name: {"status": "FAIL", "error": str(exc)} for name in ids}
    try:
        inventory_bundle = _mapping(bundles.get("inventory"))
        replay_bundle = _mapping(bundles.get("replay"))
        backfill_bundle = _mapping(bundles.get("backfill"))
        sim_bundle = _mapping(bundles.get("sim"))
        review_bundle = _mapping(bundles.get("review"))
        inventory_manifest = _mapping(
            _review_bundle_content(inventory_bundle, "replay_inventory_manifest.json")
        )
        inventory_rows = _records(
            _review_bundle_content(inventory_bundle, "replay_artifact_inventory.jsonl")
        )
        inventory_coverage = _mapping(
            _review_bundle_content(inventory_bundle, "replay_coverage_summary.json")
        )
        replay_manifest = _mapping(
            _review_bundle_content(replay_bundle, "historical_replay_manifest.json")
        )
        replay_events = _records(_review_bundle_content(replay_bundle, "replay_events.jsonl"))
        replay_summary = _mapping(
            _review_bundle_content(replay_bundle, "replay_action_summary.json")
        )
        backfill_manifest = _mapping(
            _review_bundle_content(backfill_bundle, "backfill_manifest.json")
        )
        outcome_rows = _records(
            _review_bundle_content(backfill_bundle, "replay_outcome_windows.jsonl")
        )
        sim_manifest = _mapping(
            _review_bundle_content(sim_bundle, "historical_paper_sim_manifest.json")
        )
        sim_history = _records(
            _review_bundle_content(sim_bundle, "simulated_paper_state_history.jsonl")
        )
        sim_summary = _mapping(
            _review_bundle_content(sim_bundle, "simulated_performance_summary.json")
        )
        review_manifest = _mapping(
            _review_bundle_content(review_bundle, "replay_performance_manifest.json")
        )
        calibration = _mapping(
            _review_bundle_content(review_bundle, "calibration_recommendations.json")
        )
        expected_reasons = _replay_pending_reason_summary(
            inventory_rows=inventory_rows,
            replay_summary=replay_summary,
            backfill_manifest=backfill_manifest,
            outcome_rows=outcome_rows,
            sim_summary=sim_summary,
            review_manifest=review_manifest,
        )
        expected_coverage = _replay_diagnosis_coverage_breakdown(
            inventory_manifest=inventory_manifest,
            inventory_coverage=inventory_coverage,
            replay_manifest=replay_manifest,
            replay_summary=replay_summary,
            backfill_manifest=backfill_manifest,
            sim_manifest=sim_manifest,
            sim_summary=sim_summary,
            sim_event_count=len(sim_history),
            review_manifest=review_manifest,
            calibration=calibration,
        )
        expected_health = [
            _artifact_health_row(
                "replay_inventory",
                ids["inventory"],
                Path(
                    _text(_mapping(inventory_bundle["replay_inventory_manifest.json"]).get("path"))
                ),
                inventory_manifest,
                len(inventory_rows),
                validation_status="PASS",
                source_snapshot_schema=REPLAY_INVENTORY_SNAPSHOT_SCHEMA_VERSION,
            ),
            _artifact_health_row(
                "historical_replay",
                ids["replay"],
                Path(_text(_mapping(replay_bundle["historical_replay_manifest.json"]).get("path"))),
                replay_manifest,
                len(replay_events),
                validation_status="PASS",
                source_snapshot_schema=HISTORICAL_REPLAY_SNAPSHOT_SCHEMA_VERSION,
            ),
            _artifact_health_row(
                "backfilled_outcome",
                ids["backfill"],
                Path(_text(_mapping(backfill_bundle["backfill_manifest.json"]).get("path"))),
                backfill_manifest,
                len(outcome_rows),
                validation_status="PASS",
                source_snapshot_schema=BACKFILLED_OUTCOME_SNAPSHOT_SCHEMA_VERSION,
            ),
            _artifact_health_row(
                "historical_paper_sim",
                ids["sim"],
                Path(_text(_mapping(sim_bundle["historical_paper_sim_manifest.json"]).get("path"))),
                sim_manifest,
                len(sim_history),
                validation_status="PASS",
                source_snapshot_schema=HISTORICAL_PAPER_SIM_SNAPSHOT_SCHEMA_VERSION,
            ),
            _artifact_health_row(
                "replay_performance_review",
                ids["review"],
                Path(
                    _text(_mapping(review_bundle["replay_performance_manifest.json"]).get("path"))
                ),
                review_manifest,
                len(_records(calibration.get("recommendations"))),
                validation_status="PASS",
                source_snapshot_schema=REPLAY_PERFORMANCE_REVIEW_SNAPSHOT_SCHEMA_VERSION,
            ),
        ]
        blocking = [
            row["reason"]
            for row in _records(expected_reasons.get("pending_reasons"))
            if row.get("blocking")
        ]
        expected_status = "PASS" if not blocking else "PASS_WITH_WARNINGS"
        if not inventory_rows and not replay_events and not outcome_rows:
            expected_status = "INSUFFICIENT_DATA"
        expected_manifest_fields = {
            "generated_at": snapshot.get("generated_at"),
            "inventory_id": ids["inventory"],
            "replay_id": ids["replay"],
            "backfill_id": ids["backfill"],
            "sim_id": ids["sim"],
            "review_id": ids["review"],
            "status": expected_status,
            "blocking_pending_reasons": blocking,
            "can_enter_variant_comparison": review_manifest.get("directional_evidence_ready")
            is True,
            "variant_comparison_evidence_status": (
                "READY_FOR_DIRECTIONAL_REVIEW"
                if review_manifest.get("directional_evidence_ready") is True
                else "INSUFFICIENT_DIRECTIONAL_EVIDENCE"
            ),
        }
        lineage_valid = all(
            (
                replay_manifest.get("inventory_id") == ids["inventory"],
                backfill_manifest.get("replay_id") == ids["replay"],
                sim_manifest.get("replay_id") == ids["replay"],
                review_manifest.get("replay_id") == ids["replay"],
                review_manifest.get("backfill_id") == ids["backfill"],
                review_manifest.get("sim_id") == ids["sim"],
            )
        )
        recompute_error = ""
    except Exception as exc:  # noqa: BLE001
        expected_reasons, expected_coverage, expected_health = {}, {}, []
        expected_manifest_fields, lineage_valid = {}, False
        recompute_error = str(exc)
    expected_report = render_replay_diagnosis_report(
        manifest,
        expected_coverage,
        expected_reasons,
        expected_health,
    )
    bundles_match = all(_review_bundle_matches(_mapping(bundles.get(name))) for name in ids)
    checks = [
        *shallow_checks,
        _check(
            "source_snapshot_schema_valid",
            snapshot.get("schema_version") == REPLAY_DIAGNOSIS_SNAPSHOT_SCHEMA_VERSION,
            REPLAY_DIAGNOSIS_SNAPSHOT_SCHEMA_VERSION,
        ),
        _check(
            "source_snapshot_checksum_matches",
            manifest.get("source_snapshot_checksum") == _sha256_file(snapshot_path),
            "diagnosis source snapshot",
        ),
        _check(
            "all_source_validations_pass",
            all(payload.get("status") == "PASS" for payload in source_validations.values()),
            "inventory/replay/backfill/sim/review",
        ),
        _check("source_lineage_matches", lineage_valid, ids["replay"]),
        _check("source_bundles_unchanged", bundles_match, "five source bundles"),
        _check("coverage_recomputed", coverage == expected_coverage, recompute_error),
        _check("pending_reasons_recomputed", reasons == expected_reasons, recompute_error),
        _check("artifact_health_recomputed", health == expected_health, recompute_error),
        _check(
            "manifest_derived_fields_match",
            all(manifest.get(key) == value for key, value in expected_manifest_fields.items()),
            "manifest",
        ),
        _check(
            "report_recomputed",
            (diagnosis_dir / "replay_diagnosis_report.md").is_file()
            and (diagnosis_dir / "replay_diagnosis_report.md").read_text(encoding="utf-8")
            == expected_report,
            "Markdown report",
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
    generated = _require_aware_utc(generated_at or datetime.now(UTC), "generated_at")
    source_backfill_dir = backfill_dir / backfill_id
    source_diagnosis_dir = diagnosis_dir / diagnosis_id
    backfill_validation = validate_backfill_outcome_artifact(
        backfill_id=backfill_id,
        output_dir=backfill_dir,
    )
    diagnosis_validation = validate_replay_diagnosis_artifact(
        diagnosis_id=diagnosis_id,
        output_dir=diagnosis_dir,
    )
    if backfill_validation.get("status") != "PASS" or diagnosis_validation.get("status") != "PASS":
        raise DynamicV3HistoricalReplayError(
            "backfill repair requires full-PASS backfill and diagnosis sources"
        )
    backfill_manifest = _read_json(source_backfill_dir / "backfill_manifest.json")
    diagnosis_manifest = _read_json(source_diagnosis_dir / "replay_diagnosis_manifest.json")
    original_rows = _read_jsonl(source_backfill_dir / "replay_outcome_windows.jsonl")
    replay_id = _text(backfill_manifest.get("replay_id"))
    source_replay_dir = replay_dir / replay_id
    replay_validation = validate_historical_replay_artifact(
        replay_id=replay_id,
        output_dir=replay_dir,
    )
    if replay_validation.get("status") != "PASS":
        raise DynamicV3HistoricalReplayError("backfill repair requires a full-PASS replay source")
    replay_manifest = _read_json(source_replay_dir / "historical_replay_manifest.json")
    if not all(
        (
            diagnosis_manifest.get("backfill_id") == backfill_id,
            diagnosis_manifest.get("replay_id") == replay_id,
            backfill_manifest.get("replay_id") == replay_id,
        )
    ):
        raise DynamicV3HistoricalReplayError("backfill repair source lineage is inconsistent")
    source_times = [
        _datetime_from_any(backfill_manifest.get("generated_at")),
        _datetime_from_any(diagnosis_manifest.get("generated_at")),
        _datetime_from_any(replay_manifest.get("generated_at")),
    ]
    if any(value is None for value in source_times) or any(
        generated < value for value in source_times if value is not None
    ):
        raise DynamicV3HistoricalReplayError(
            "backfill repair generated_at must not precede source artifacts"
        )
    replay_events = _read_jsonl(source_replay_dir / "replay_events.jsonl")
    quality = None
    if enforce_data_quality_gate:
        quality = _validate_cached_data_quality(
            as_of=generated.date(),
            prices_path=prices_path,
            rates_path=rates_path,
        )
        if not quality.passed:
            raise DynamicV3HistoricalReplayError(
                f"backfill repair data quality gate failed: {quality.status}"
            )
    prices = _load_prices_for_replay(prices_path, replay_events)
    price_rows = _frozen_replay_price_rows(prices, generated_date=generated.date())
    frozen_prices = pd.DataFrame(
        price_rows,
        columns=["symbol", "date", "adj_close"],
    ).rename(columns={"date": "_date", "adj_close": "_adj_close"})
    frozen_prices["_date"] = pd.to_datetime(frozen_prices["_date"]).dt.date
    price_dates = _available_price_dates(frozen_prices)
    event_map = {_text(event.get("replay_event_id")): event for event in replay_events}
    repaired_rows, actions = _repair_outcome_rows(
        original_rows=original_rows,
        event_map=event_map,
        prices=frozen_prices,
        price_dates=price_dates,
        generated_date=generated.date(),
        cost_rate=_float(backfill_manifest.get("cost_rate")),
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
        "sample_unit": "event_variant_window",
        "before": before,
        "after": after,
        "repaired_count": repaired_count,
        "still_pending_count": after["pending"],
        "still_insufficient_count": after["insufficient_data"],
    }
    status = "PASS" if repaired_count else "PASS_WITH_WARNINGS"
    if not repaired_rows or after["available"] == 0:
        status = "INSUFFICIENT_DATA" if not after["pending"] else "PENDING"
    repair_id = _stable_id("backfill-repair", backfill_id, diagnosis_id, generated.isoformat())
    repair_dir = _unique_dir(output_dir / repair_id)
    repair_dir.mkdir(parents=True, exist_ok=False)
    quality_status = "SKIPPED_EXPLICIT_TEST_FIXTURE"
    quality_report_path = ""
    if enforce_data_quality_gate:
        assert quality is not None
        quality_report = repair_dir / "validate_data_quality_report.md"
        write_data_quality_report(quality, quality_report)
        quality_status = quality.status
        quality_report_path = str(quality_report)
    source_snapshot = {
        "schema_version": BACKFILL_REPAIR_SNAPSHOT_SCHEMA_VERSION,
        "repair_id": repair_dir.name,
        "generated_at": generated.isoformat(),
        "backfill_id": backfill_id,
        "diagnosis_id": diagnosis_id,
        "replay_id": replay_id,
        "backfill_root": str(source_backfill_dir),
        "diagnosis_root": str(source_diagnosis_dir),
        "replay_root": str(source_replay_dir),
        "backfill_files": _review_source_bundle(
            source_backfill_dir,
            (
                "backfilled_outcome_source_snapshot.json",
                "backfill_manifest.json",
                "replay_outcome_windows.jsonl",
                "variant_performance_summary.json",
                "backfill_outcome_report.md",
            ),
        ),
        "diagnosis_files": _review_source_bundle(
            source_diagnosis_dir,
            (
                "replay_diagnosis_source_snapshot.json",
                "replay_diagnosis_manifest.json",
                "replay_coverage_breakdown.json",
                "replay_pending_reason_summary.json",
                "replay_artifact_health_matrix.jsonl",
                "replay_diagnosis_report.md",
            ),
        ),
        "replay_files": _review_source_bundle(
            source_replay_dir,
            (
                "historical_replay_source_snapshot.json",
                "historical_replay_manifest.json",
                "replay_events.jsonl",
                "replay_decision_inputs.jsonl",
                "replay_action_summary.json",
                "historical_replay_report.md",
            ),
        ),
        "cost_rate": _float(backfill_manifest.get("cost_rate")),
        "prices_path": str(prices_path),
        "prices_checksum": _sha256_file(prices_path),
        "rates_path": str(rates_path),
        "rates_checksum": _sha256_file(rates_path),
        "price_rows": price_rows,
        "data_quality_status": quality_status,
        "data_quality_gate_skipped_for_test": not enforce_data_quality_gate,
    }
    source_snapshot_path = repair_dir / "backfill_repair_source_snapshot.json"
    _write_json(source_snapshot_path, source_snapshot)
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
        "source_snapshot_path": str(source_snapshot_path),
        "source_snapshot_checksum": _sha256_file(source_snapshot_path),
        "future_data_used_in_decision": False,
        "repair_count_unit": "event_variant_window",
        "source_backfill_validation_status": "PASS",
        "source_diagnosis_validation_status": "PASS",
        "source_replay_validation_status": "PASS",
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
        "source_snapshot": source_snapshot,
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
    delta = _read_optional_json(repair_dir / "backfill_availability_delta.json") or {}
    shallow_checks = [
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
    snapshot_path = repair_dir / "backfill_repair_source_snapshot.json"
    if not snapshot_path.is_file():
        payload = _validation_payload(
            report_type="etf_dynamic_v3_backfill_repair_validation",
            artifact_id_key="repair_id",
            artifact_id=repair_id,
            checks=shallow_checks,
        )
        if payload["status"] == "PASS":
            payload["status"] = "PASS_WITH_WARNINGS"
        payload["source_snapshot_status"] = "LEGACY_UNSNAPSHOTTED"
        return payload
    snapshot = _read_optional_json(snapshot_path) or {}
    backfill_bundle = _mapping(snapshot.get("backfill_files"))
    diagnosis_bundle = _mapping(snapshot.get("diagnosis_files"))
    replay_bundle = _mapping(snapshot.get("replay_files"))
    backfill_root = Path(_text(snapshot.get("backfill_root")))
    diagnosis_root = Path(_text(snapshot.get("diagnosis_root")))
    replay_root = Path(_text(snapshot.get("replay_root")))
    try:
        validations = {
            "backfill": validate_backfill_outcome_artifact(
                backfill_id=_text(snapshot.get("backfill_id")),
                output_dir=backfill_root.parent,
            ),
            "diagnosis": validate_replay_diagnosis_artifact(
                diagnosis_id=_text(snapshot.get("diagnosis_id")),
                output_dir=diagnosis_root.parent,
            ),
            "replay": validate_historical_replay_artifact(
                replay_id=_text(snapshot.get("replay_id")),
                output_dir=replay_root.parent,
            ),
        }
    except Exception as exc:  # noqa: BLE001
        validations = {
            name: {"status": "FAIL", "error": str(exc)}
            for name in (
                "backfill",
                "diagnosis",
                "replay",
            )
        }
    prices_path = Path(_text(snapshot.get("prices_path")))
    rates_path = Path(_text(snapshot.get("rates_path")))
    source_files_match = (
        _review_bundle_matches(backfill_bundle)
        and _review_bundle_matches(diagnosis_bundle)
        and _review_bundle_matches(replay_bundle)
        and prices_path.is_file()
        and rates_path.is_file()
        and snapshot.get("prices_checksum") == _sha256_file(prices_path)
        and snapshot.get("rates_checksum") == _sha256_file(rates_path)
    )
    generated = _datetime_from_any(snapshot.get("generated_at"))
    dq_valid = snapshot.get("data_quality_status") == "SKIPPED_EXPLICIT_TEST_FIXTURE"
    if snapshot.get("data_quality_gate_skipped_for_test") is not True and generated is not None:
        quality = _validate_cached_data_quality(
            as_of=generated.date(),
            prices_path=prices_path,
            rates_path=rates_path,
        )
        dq_valid = quality.passed and quality.status == snapshot.get("data_quality_status")
    try:
        original_rows = _records(
            _review_bundle_content(backfill_bundle, "replay_outcome_windows.jsonl")
        )
        backfill_manifest = _mapping(
            _review_bundle_content(backfill_bundle, "backfill_manifest.json")
        )
        diagnosis_manifest = _mapping(
            _review_bundle_content(diagnosis_bundle, "replay_diagnosis_manifest.json")
        )
        replay_manifest = _mapping(
            _review_bundle_content(replay_bundle, "historical_replay_manifest.json")
        )
        replay_events = _records(_review_bundle_content(replay_bundle, "replay_events.jsonl"))
        if generated is None:
            raise DynamicV3HistoricalReplayError("repair snapshot generated_at is invalid")
        frozen_prices = pd.DataFrame(
            _records(snapshot.get("price_rows")),
            columns=["symbol", "date", "adj_close"],
        ).rename(columns={"date": "_date", "adj_close": "_adj_close"})
        frozen_prices["_date"] = pd.to_datetime(frozen_prices["_date"]).dt.date
        expected_rows, expected_actions = _repair_outcome_rows(
            original_rows=original_rows,
            event_map={_text(event.get("replay_event_id")): event for event in replay_events},
            prices=frozen_prices,
            price_dates=_available_price_dates(frozen_prices),
            generated_date=generated.date(),
            cost_rate=_float(snapshot.get("cost_rate")),
        )
        before = _availability_counts(original_rows)
        after = _availability_counts(expected_rows)
        repaired_count = sum(
            1
            for action in expected_actions
            if action.get("original_status") != action.get("new_status")
            and action.get("new_status") == "AVAILABLE"
        )
        expected_delta = {
            "sample_unit": "event_variant_window",
            "before": before,
            "after": after,
            "repaired_count": repaired_count,
            "still_pending_count": after["pending"],
            "still_insufficient_count": after["insufficient_data"],
        }
        expected_status = "PASS" if repaired_count else "PASS_WITH_WARNINGS"
        if not expected_rows or after["available"] == 0:
            expected_status = "INSUFFICIENT_DATA" if not after["pending"] else "PENDING"
        lineage_valid = all(
            (
                diagnosis_manifest.get("backfill_id") == snapshot.get("backfill_id"),
                diagnosis_manifest.get("replay_id") == snapshot.get("replay_id"),
                backfill_manifest.get("replay_id") == snapshot.get("replay_id"),
                replay_manifest.get("replay_id") == snapshot.get("replay_id"),
            )
        )
        expected_manifest_fields = {
            "repair_id": repair_id,
            "backfill_id": snapshot.get("backfill_id"),
            "diagnosis_id": snapshot.get("diagnosis_id"),
            "replay_id": snapshot.get("replay_id"),
            "generated_at": snapshot.get("generated_at"),
            "status": expected_status,
            "data_quality_status": snapshot.get("data_quality_status"),
            "repair_count_unit": "event_variant_window",
            "source_backfill_validation_status": "PASS",
            "source_diagnosis_validation_status": "PASS",
            "source_replay_validation_status": "PASS",
        }
        recompute_error = ""
    except Exception as exc:  # noqa: BLE001
        expected_rows, expected_actions, expected_delta = [], [], {}
        expected_manifest_fields, lineage_valid = {}, False
        recompute_error = str(exc)
    expected_report = render_backfill_repair_report(
        manifest,
        expected_delta,
        expected_actions,
    )
    checks = [
        *shallow_checks,
        _check(
            "source_snapshot_schema_valid",
            snapshot.get("schema_version") == BACKFILL_REPAIR_SNAPSHOT_SCHEMA_VERSION,
            BACKFILL_REPAIR_SNAPSHOT_SCHEMA_VERSION,
        ),
        _check(
            "source_snapshot_checksum_matches",
            manifest.get("source_snapshot_checksum") == _sha256_file(snapshot_path),
            "repair source snapshot",
        ),
        _check(
            "all_source_validations_pass",
            all(payload.get("status") == "PASS" for payload in validations.values()),
            "backfill/diagnosis/replay",
        ),
        _check("source_lineage_matches", lineage_valid, _text(snapshot.get("replay_id"))),
        _check("source_files_unchanged", source_files_match, "source bundles and caches"),
        _check("data_quality_evidence_valid", dq_valid, _text(snapshot.get("data_quality_status"))),
        _check("repair_actions_recomputed", actions == expected_actions, recompute_error),
        _check("repaired_rows_recomputed", rows == expected_rows, recompute_error),
        _check("availability_delta_recomputed", delta == expected_delta, recompute_error),
        _check(
            "manifest_derived_fields_match",
            all(manifest.get(key) == value for key, value in expected_manifest_fields.items()),
            "manifest",
        ),
        _check(
            "original_available_rows_immutable",
            all(
                row == expected_rows[index]
                for index, row in enumerate(original_rows)
                if row.get("outcome_status") == "AVAILABLE"
            ),
            "AVAILABLE rows",
        ),
        _check(
            "report_recomputed",
            (repair_dir / "backfill_repair_report.md").is_file()
            and (repair_dir / "backfill_repair_report.md").read_text(encoding="utf-8")
            == expected_report,
            "Markdown report",
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
            f"- cutoff后来源排除数：{manifest.get('future_generated_source_excluded_count')}",
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
            f"- source inventory：`{manifest.get('inventory_id')}`",
            f"- inventory validation：{manifest.get('source_inventory_validation_status')}",
            f"- inventory evidence cutoff：{manifest.get('source_inventory_evidence_cutoff')}",
            f"- market regime：{manifest.get('market_regime')}",
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
            "- turnover convention：one_way_l1_weight_change = Σ|w_variant-w_current|。",
            "- owner/paper source_status会显式区分recorded action与"
            "missing-input no-trade fallback。",
            "- future_prices_used_for_decision_input=false；本层不读取outcome price。",
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
            f"- source_replay_validation_status：{manifest.get('source_replay_validation_status')}",
            "- sample_unit：event × variant × configured trading-session window。",
            f"- cost_rate：{manifest.get('cost_rate')}；cost_role：{manifest.get('cost_role')}。",
            f"- policy：{manifest.get('policy_id')}@{manifest.get('policy_version')}；"
            f"session_source：{manifest.get('session_source')}。",
            "- return = gross fixed-share return - initial one-way L1 turnover cost；"
            "max_drawdown / realized_volatility来自gross price path，未逐日摊销成本。",
            "- PENDING / INSUFFICIENT_DATA 的gross/net return、relative、drawdown、"
            "volatility均为null，不以0伪装可观测结果。",
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
            f"- estimated_cost：{summary.get('estimated_cost')}；cost_rate："
            f"{manifest.get('cost_rate')}",
            f"- trade_count：{summary.get('trade_count')}",
            f"- relative_to_no_trade：{summary.get('relative_to_no_trade')}",
            f"- daily_return_observation_count：{summary.get('daily_return_observation_count')}",
            f"- data_quality_status：{manifest.get('data_quality_status')}；source replay："
            f"{manifest.get('source_replay_validation_status')}",
            "- 每个event interval使用fixed-share value path；event date按simulated before"
            "到frozen variant target重置并扣one-way L1成本。",
            "- 缺required-symbol path时status=INSUFFICIENT_DATA，return/risk/relative为null，"
            "不以0继续累计。",
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
            f"- limited_adjustment_vs_no_trade：{manifest.get('limited_adjustment_vs_no_trade')}",
            f"- simulation_variant：{sim_summary.get('variant')}",
            f"- simulation_total_return：{sim_summary.get('total_return')}",
            f"- policy：{manifest.get('policy_id')}@{manifest.get('policy_version')}",
            f"- directional_evidence_ready：{manifest.get('directional_evidence_ready')}",
            f"- distinct_replay_event_count：{manifest.get('distinct_replay_event_count')}",
            f"- available_comparison_window_count："
            f"{manifest.get('available_comparison_window_count')}",
            f"- primary_recommendation：{top.get('type')}",
            f"- reason：{top.get('reason')}",
            f"- requires_owner_approval：{top.get('requires_owner_approval')}",
            f"- recommendation_effectiveness_count："
            f"{len(_records(effectiveness.get('recommendation_effectiveness')))}",
            "- relative return只能支持相对no_trade的正/非正率；没有独立标签时，"
            "false alarm与missed opportunity保持null。",
            "- 未通过distinct-event/window样本门槛时只允许continue_forward_tracking。",
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
            f"- limited_adjustment_vs_no_trade: {manifest.get('limited_adjustment_vs_no_trade')}",
            f"- directional_evidence_ready: {manifest.get('directional_evidence_ready')}",
            f"- evidence_events/windows: {manifest.get('distinct_replay_event_count')} / "
            f"{manifest.get('available_comparison_window_count')}",
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
            f"- variant_comparison_evidence_status："
            f"{manifest.get('variant_comparison_evidence_status')}",
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
            "- pending reason counts按inventory event、outcome variant-window和chain状态"
            "分别披露单位，不把不同样本单位解释为同一失败率。",
            "- artifact health要求五条source validator PASS、snapshot schema与manifest checksum。",
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
            normalized["limited_adjustment_avg_return_delta"] = -_float(row.get("avg_return_delta"))
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
    units: dict[str, Counter[str]] = defaultdict(Counter)
    for row in inventory_rows:
        limitations = set(_texts(row.get("replay_limitations")))
        if row.get("pit_safety_status") == "PIT_UNSAFE":
            counter["pit_unsafe"] += 1
            units["pit_unsafe"]["inventory_event"] += 1
        if "MISSING_TARGET_WEIGHTS" in limitations:
            counter["missing_target_weights"] += 1
            units["missing_target_weights"]["inventory_event"] += 1
        if "MISSING_PRICE_DATA" in limitations:
            counter["missing_price_data"] += 1
            units["missing_price_data"]["inventory_event"] += 1
    if _int(replay_summary.get("replay_event_count")) == 0:
        counter["insufficient_replay_events"] += 1
        units["insufficient_replay_events"]["replay_chain"] += 1
    for row in outcome_rows:
        status = _text(row.get("outcome_status"))
        if status == "PENDING":
            counter["future_window_not_reached"] += 1
            units["future_window_not_reached"]["outcome_variant_window"] += 1
        elif status == "INSUFFICIENT_DATA":
            counter["missing_price_data"] += 1
            units["missing_price_data"]["outcome_variant_window"] += 1
    if _int(backfill_manifest.get("available_count")) == 0:
        counter["no_available_outcome_windows"] += 1
        units["no_available_outcome_windows"]["backfill_chain"] += 1
    if _text(sim_summary.get("simulation_status")) == "INSUFFICIENT_DATA":
        counter["paper_sim_insufficient_data"] += 1
        units["paper_sim_insufficient_data"]["simulation_chain"] += 1
    if (
        _text(review_manifest.get("status")) in {"PENDING", "INSUFFICIENT_DATA"}
        and _int(backfill_manifest.get("available_count")) == 0
    ):
        counter["review_waiting_for_backfill"] += 1
        units["review_waiting_for_backfill"]["review_chain"] += 1
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_replay_pending_reason_summary",
        "pending_reasons": [
            {
                "reason": reason,
                "count": count,
                "count_units": dict(sorted(units[reason].items())),
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
    *,
    validation_status: str,
    source_snapshot_schema: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "artifact_type": artifact_type,
        "artifact_id": artifact_id,
        "path": str(path),
        "exists": path.exists(),
        "status": payload.get("status", "MISSING"),
        "record_count": record_count,
        "record_count_unit": {
            "replay_inventory": "inventory_event",
            "historical_replay": "replay_event",
            "backfilled_outcome": "outcome_variant_window",
            "historical_paper_sim": "simulation_state",
            "replay_performance_review": "recommendation",
        }.get(artifact_type, "record"),
        "validation_status": validation_status,
        "source_snapshot_schema": source_snapshot_schema,
        "manifest_checksum": _sha256_file(path) if path.is_file() else "",
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
    cost_rate: float,
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
            cost_rate=cost_rate,
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


def _historical_replay_views(
    inventory_rows: Sequence[Mapping[str, Any]],
    *,
    include_pit_warning: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    events = []
    decision_inputs = []
    skipped = []
    for row in inventory_rows:
        pit_status = _text(row.get("pit_safety_status"))
        eligibility = _text(row.get("replay_eligibility"))
        hard_limitations = HARD_PIT_LIMITATIONS & set(_texts(row.get("replay_limitations")))
        if pit_status == "PIT_UNSAFE" or eligibility == "INELIGIBLE" or hard_limitations:
            skipped.append(_skip_row(row, "PIT_UNSAFE_EXCLUDED"))
            continue
        if (pit_status, eligibility) not in {
            ("PIT_SAFE", "ELIGIBLE"),
            ("PIT_WARNING", "PARTIAL"),
        }:
            raise DynamicV3HistoricalReplayError(
                "inventory PIT status/eligibility binding is invalid: "
                f"{row.get('daily_advisory_id')}"
            )
        if pit_status == "PIT_WARNING" and not include_pit_warning:
            skipped.append(_skip_row(row, "PIT_WARNING_EXCLUDED"))
            continue
        event = _historical_replay_event(row)
        events.append(event)
        decision_inputs.append(_historical_decision_input(row, event))
    return events, decision_inputs, skipped


def _validated_replay_weights(value: Any, field: str) -> dict[str, float]:
    raw = _mapping(value)
    if not raw:
        raise DynamicV3HistoricalReplayError(f"historical replay weights are missing: {field}")
    weights = {_text(symbol): _float(weight) for symbol, weight in raw.items()}
    if (
        any(not symbol for symbol in weights)
        or any(not math.isfinite(weight) or weight < 0 for weight in weights.values())
        or abs(sum(weights.values()) - 1.0) > 1e-6
    ):
        raise DynamicV3HistoricalReplayError(
            f"historical replay weights must be a finite nonnegative simplex: {field}"
        )
    return {symbol: round(weight, 6) for symbol, weight in sorted(weights.items())}


def _historical_replay_event(row: Mapping[str, Any]) -> dict[str, Any]:
    inputs = _mapping(row.get("decision_inputs"))
    current = _validated_replay_weights(inputs.get("current_weights"), "current_weights")
    consensus = _validated_replay_weights(inputs.get("consensus_weights"), "consensus_weights")
    limited = _validated_replay_weights(
        inputs.get("limited_adjustment_weights"),
        "limited_adjustment_weights",
    )
    owner_raw = _mapping(inputs.get("owner_decision_weights"))
    paper_raw = _mapping(inputs.get("paper_action_weights"))
    owner_recorded = _text(inputs.get("owner_decision"), "missing") != "missing"
    owner_weights = (
        _validated_replay_weights(owner_raw, "owner_decision_weights") if owner_raw else current
    )
    paper_weights = (
        _validated_replay_weights(paper_raw, "paper_action_weights") if paper_raw else current
    )
    variants = [
        _variant("no_trade", current, "Keep current weights unchanged", "CURRENT_WEIGHTS"),
        _variant(
            "consensus_target",
            consensus,
            "Move fully to consensus target weights",
            "RECORDED_CONSENSUS_TARGET",
        ),
        _variant(
            "limited_adjustment",
            limited,
            "Apply capped advisory deltas",
            "RECORDED_LIMITED_ADJUSTMENT",
        ),
        _variant(
            "owner_decision",
            owner_weights,
            "Apply recorded owner decision if available",
            "RECORDED_OWNER_DECISION" if owner_recorded else "FALLBACK_NO_TRADE_MISSING_OWNER",
        ),
        _variant(
            "paper_action",
            paper_weights,
            "Apply recorded paper action if available",
            "RECORDED_PAPER_ACTION" if paper_raw else "FALLBACK_NO_TRADE_MISSING_PAPER_ACTION",
        ),
    ]
    for variant in variants:
        weights = _mapping(variant.get("weights"))
        variant["turnover"] = (
            round(sum(abs(value) for value in _weight_deltas(current, weights).values()), 6)
            if weights
            else 0.0
        )
        variant["turnover_convention"] = "one_way_l1_weight_change"
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


def _backfill_views(
    replay_events: Sequence[Mapping[str, Any]],
    *,
    windows: Sequence[int],
    prices: pd.DataFrame,
    generated_date: date,
    cost_rate: float,
) -> tuple[list[dict[str, Any]], dict[str, Any], Counter[str], str]:
    price_dates = _available_price_dates(prices)
    rows = []
    for event in replay_events:
        rows.extend(
            _backfilled_outcome_rows(
                event=event,
                windows=windows,
                prices=prices,
                price_dates=price_dates,
                generated_date=generated_date,
                cost_rate=cost_rate,
            )
        )
    summary = _variant_performance_summary(rows)
    rollup = Counter(_text(row.get("outcome_status")) for row in rows)
    status = "AVAILABLE" if rollup.get("AVAILABLE") else "INSUFFICIENT_DATA"
    if rollup.get("PENDING") and not rollup.get("AVAILABLE"):
        status = "PENDING"
    return rows, summary, rollup, status


def _backfilled_outcome_rows(
    *,
    event: Mapping[str, Any],
    windows: Sequence[int],
    prices: pd.DataFrame,
    price_dates: Sequence[date],
    generated_date: date,
    cost_rate: float,
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
                        cost_rate=cost_rate,
                    )
                )
            continue
        returns = {}
        for variant in variants:
            name = _text(variant.get("variant"))
            metrics = _portfolio_metrics(prices, variant_weights.get(name, {}), start, end)
            turnover = _float(_mapping(variant).get("turnover"))
            if metrics.get("status") == "AVAILABLE":
                gross_return = _float(metrics.get("return"))
                estimated_cost = round(turnover * cost_rate, 6)
                metrics = {
                    **metrics,
                    "gross_return": gross_return,
                    "estimated_cost": estimated_cost,
                    "return": round(gross_return - estimated_cost, 6),
                }
            else:
                metrics = {
                    **metrics,
                    "gross_return": None,
                    "estimated_cost": None,
                }
            returns[name] = metrics
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
                    "gross_return": metrics["gross_return"],
                    "estimated_cost": metrics["estimated_cost"],
                    "return": metrics["return"],
                    "relative_to_no_trade": (
                        round(
                            metrics["return"]
                            - returns.get("no_trade", _missing_metrics())["return"],
                            6,
                        )
                        if status == "AVAILABLE"
                        and returns.get("no_trade", {}).get("status") == "AVAILABLE"
                        else None
                    ),
                    "relative_to_consensus_target": (
                        round(
                            metrics["return"]
                            - returns.get("consensus_target", _missing_metrics())["return"],
                            6,
                        )
                        if status == "AVAILABLE"
                        and returns.get("consensus_target", {}).get("status") == "AVAILABLE"
                        else None
                    ),
                    "relative_to_limited_adjustment": (
                        round(
                            metrics["return"]
                            - returns.get("limited_adjustment", _missing_metrics())["return"],
                            6,
                        )
                        if status == "AVAILABLE"
                        and returns.get("limited_adjustment", {}).get("status") == "AVAILABLE"
                        else None
                    ),
                    "max_drawdown": metrics["max_drawdown"],
                    "realized_volatility": metrics["realized_volatility"],
                    "turnover": _float(_mapping(variant).get("turnover")),
                    "turnover_convention": "one_way_l1_weight_change",
                    "cost_rate": cost_rate,
                    "cost_role": "initial_turnover_deduction_from_return",
                    "risk_metric_cost_role": "gross_price_path_cost_not_applied",
                    "outcome_status": status,
                    "outcome_reason": (
                        "required_symbol_price_path_incomplete"
                        if status == "INSUFFICIENT_DATA"
                        else ""
                    ),
                    "broker_action_taken": False,
                }
            )
    return rows


def _simulate_paper_history(
    events: Sequence[Mapping[str, Any]],
    *,
    variant: str,
    prices: pd.DataFrame,
    cost_rate: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    history, ledger, summary = _simulate_variant_history(
        events,
        variant=variant,
        prices=prices,
        cost_rate=cost_rate,
    )
    _, _, baseline = _simulate_variant_history(
        events,
        variant="no_trade_baseline",
        prices=prices,
        cost_rate=cost_rate,
    )
    if (
        summary.get("simulation_status") == "AVAILABLE"
        and baseline.get("simulation_status") == "AVAILABLE"
    ):
        relative = round(
            _float(summary.get("total_return")) - _float(baseline.get("total_return")),
            6,
        )
        summary["relative_to_no_trade"] = relative
        summary["relative_to_baseline"] = relative
    return history, ledger, summary


def _simulate_variant_history(
    events: Sequence[Mapping[str, Any]],
    *,
    variant: str,
    prices: pd.DataFrame,
    cost_rate: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    if not events:
        return [], [], _empty_sim_summary(variant)
    replay_variant = "no_trade" if variant == "no_trade_baseline" else variant
    ordered_events = sorted(events, key=lambda row: _text(row.get("as_of")))
    start_date = _date_from_any(ordered_events[0].get("as_of"))
    if start_date is None:
        return [], [], _empty_sim_summary(variant)
    current_weights = _normalize_weights(_mapping(ordered_events[0].get("current_weights")))
    if not current_weights:
        return [], [], _empty_sim_summary(variant)
    history: list[dict[str, Any]] = []
    ledger: list[dict[str, Any]] = []
    daily_returns: list[float] = []
    value = 1.0
    previous_date = start_date
    incomplete_reason = ""
    for event in ordered_events:
        event_date = _date_from_any(event.get("as_of"))
        if event_date is None or event_date < previous_date:
            incomplete_reason = "invalid_or_non_monotonic_event_date"
            break
        period_gross_return = 0.0
        interval_daily_returns: list[float] = []
        if event_date > previous_date:
            try:
                period_gross_return, interval_daily_returns = _portfolio_return_and_path(
                    prices,
                    current_weights,
                    previous_date,
                    event_date,
                )
            except DynamicV3HistoricalReplayError:
                incomplete_reason = "required_symbol_price_path_incomplete"
                break
            value = value * (1.0 + period_gross_return)
            daily_returns.extend(interval_daily_returns)
        after = _variant_weights(event, replay_variant)
        if not after:
            incomplete_reason = "variant_target_weights_missing"
            break
        deltas = _weight_deltas(current_weights, after)
        turnover = round(sum(abs(delta) for delta in deltas.values()), 6)
        estimated_cost = round(turnover * cost_rate, 8)
        value_before_trade = value
        value = value * (1.0 - estimated_cost)
        if estimated_cost:
            if interval_daily_returns and daily_returns:
                daily_returns[-1] = (1.0 + daily_returns[-1]) * (1.0 - estimated_cost) - 1.0
            else:
                daily_returns.append(-estimated_cost)
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
                    "turnover_convention": "one_way_l1_weight_change",
                    "cost_rate": cost_rate,
                    "estimated_cost": estimated_cost,
                    "cost_role": "event_target_reset_cost_on_pre_trade_equity",
                    "reason": "historical_replay_event_target_reset",
                    "source_replay_event_id": event.get("replay_event_id"),
                    "broker_action_taken": False,
                }
            )
        current_weights = after
        history.append(
            {
                "schema_version": SCHEMA_VERSION,
                "date": event_date.isoformat(),
                "variant": variant,
                "weights": current_weights,
                "portfolio_value_before_trade": round(value_before_trade, 8),
                "portfolio_value": round(value, 8),
                "period_gross_return": round(period_gross_return, 6),
                "period_observation_days": len(interval_daily_returns),
                "estimated_cost": estimated_cost,
                "turnover": turnover,
                "turnover_convention": "one_way_l1_weight_change",
                "source_replay_event_id": event.get("replay_event_id"),
                "broker_action_taken": False,
            }
        )
        previous_date = event_date
    if incomplete_reason or len({row["date"] for row in history}) < 2:
        summary = _empty_sim_summary(variant)
        summary.update(
            {
                "start_date": start_date.isoformat(),
                "end_date": history[-1]["date"] if history else "",
                "simulation_reason": incomplete_reason or "fewer_than_two_event_dates",
                "turnover": round(sum(_float(row.get("turnover")) for row in ledger), 6),
                "estimated_cost": round(
                    sum(_float(row.get("estimated_cost")) for row in ledger),
                    8,
                ),
                "trade_count": len(ledger),
            }
        )
        return history, ledger, summary
    end_date = _date_from_any(history[-1]["date"])
    assert end_date is not None
    total_return = round(value - 1.0, 6)
    years = max((end_date - start_date).days / 365.25, 0.0)
    annualized = round(value ** (1 / years) - 1.0, 6) if years > 0 and value > 0 else None
    summary = {
        "schema_version": SCHEMA_VERSION,
        "variant": variant,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "total_return": total_return,
        "annualized_return": annualized,
        "max_drawdown": round(_max_drawdown(daily_returns), 6),
        "realized_volatility": round(_realized_volatility(daily_returns), 6),
        "daily_return_observation_count": len(daily_returns),
        "turnover": round(sum(_float(row.get("turnover")) for row in ledger), 6),
        "estimated_cost": round(sum(_float(row.get("estimated_cost")) for row in ledger), 8),
        "cost_rate": cost_rate,
        "cost_role": "event_target_reset_cost_on_pre_trade_equity",
        "trade_count": len(ledger),
        "relative_to_no_trade": None,
        "relative_to_baseline": None,
        "simulation_status": "AVAILABLE",
        "simulation_reason": "",
        "outcome_mode": OUTCOME_MODE_HISTORICAL_REPLAY,
        "sample_unit": "daily_price_return_with_event_target_resets",
        "broker_action_taken": False,
    }
    return history, ledger, summary


def _advisory_rule_effectiveness(
    rows: Sequence[Mapping[str, Any]],
    sim_summary: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    available = [row for row in rows if row.get("outcome_status") == "AVAILABLE"]
    comparison_window = _int(
        _mapping(policy.get("evidence_gate")).get("comparison_window_trading_days")
    )
    by_action: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in available:
        by_action[_text(row.get("recommended_action"), "MISSING")].append(row)
    recommendation_effectiveness = []
    for action, action_rows in sorted(by_action.items()):
        rel_5 = _finite_window_values(action_rows, 5, "relative_to_no_trade")
        rel_20 = _finite_window_values(action_rows, 20, "relative_to_no_trade")
        recommendation_effectiveness.append(
            {
                "recommended_action": action,
                "event_count": len({_text(row.get("replay_event_id")) for row in action_rows}),
                "available_5d_window_count": len(rel_5),
                "available_20d_window_count": len(rel_20),
                "avg_relative_to_no_trade_5d": _rounded_avg_or_none(rel_5),
                "avg_relative_to_no_trade_20d": _rounded_avg_or_none(rel_20),
                "nonpositive_rate_vs_no_trade_5d": (
                    round(sum(1 for value in rel_5 if value <= 0) / len(rel_5), 6)
                    if rel_5
                    else None
                ),
                "positive_rate_vs_no_trade_5d": (
                    round(sum(1 for value in rel_5 if value > 0) / len(rel_5), 6) if rel_5 else None
                ),
                "false_alarm_rate": None,
                "missed_opportunity_rate": None,
                "classification_metric_status": "NOT_IDENTIFIED_FROM_RELATIVE_RETURN_ONLY",
            }
        )
    by_variant: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in available:
        by_variant[_text(row.get("variant"), "MISSING")].append(row)
    variant_effectiveness = []
    for variant, variant_rows in sorted(by_variant.items()):
        rel = _finite_window_values(
            variant_rows,
            comparison_window,
            "relative_to_no_trade",
        )
        drawdown = _finite_window_values(variant_rows, 20, "max_drawdown")
        turnover = [
            float(row["turnover"])
            for row in variant_rows
            if isinstance(row.get("turnover"), (int, float))
            and not isinstance(row.get("turnover"), bool)
            and math.isfinite(float(row["turnover"]))
        ]
        variant_effectiveness.append(
            {
                "variant": variant,
                "event_count": len({_text(row.get("replay_event_id")) for row in variant_rows}),
                "available_comparison_window_count": len(rel),
                "win_rate_vs_no_trade": (
                    round(sum(1 for value in rel if value > 0) / len(rel), 6) if rel else None
                ),
                "avg_return_delta": _rounded_avg_or_none(rel),
                "avg_max_drawdown_20d": _rounded_avg_or_none(drawdown),
                "avg_turnover": _rounded_avg_or_none(turnover),
                "simulated_total_turnover": (
                    sim_summary.get("turnover") if variant == sim_summary.get("variant") else None
                ),
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_advisory_rule_effectiveness",
        "recommendation_effectiveness": recommendation_effectiveness,
        "variant_effectiveness": variant_effectiveness,
        "comparison_window_trading_days": comparison_window,
        "metric_semantics": (
            "relative return signs support positive/nonpositive rates only; "
            "false alarm and missed opportunity require independent labels"
        ),
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _calibration_recommendations(
    rows: Sequence[Mapping[str, Any]],
    variant_summary: Mapping[str, Any],
    sim_summary: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    gate = _mapping(policy.get("evidence_gate"))
    metadata = _mapping(policy.get("policy_metadata"))
    comparison_window = _int(gate.get("comparison_window_trading_days"))
    available_rows = [
        row
        for row in rows
        if row.get("outcome_status") == "AVAILABLE"
        and _int(row.get("window_days")) == comparison_window
        and isinstance(row.get("relative_to_no_trade"), (int, float))
        and not isinstance(row.get("relative_to_no_trade"), bool)
        and math.isfinite(float(row["relative_to_no_trade"]))
    ]
    distinct_events = len({_text(row.get("replay_event_id")) for row in available_rows})
    required_events = _int(gate.get("minimum_distinct_replay_event_count"))
    required_windows = _int(gate.get("minimum_available_variant_window_count"))
    sim_available = sim_summary.get("simulation_status") == "AVAILABLE"
    directional_ready = (
        distinct_events >= required_events
        and len(available_rows) >= required_windows
        and (sim_available or gate.get("require_historical_paper_sim_available") is not True)
    )
    best = _text(variant_summary.get("best_variant"), "MISSING")
    limited_raw = variant_summary.get("limited_adjustment_vs_no_trade_5d")
    limited_delta = (
        float(limited_raw)
        if isinstance(limited_raw, (int, float))
        and not isinstance(limited_raw, bool)
        and math.isfinite(float(limited_raw))
        else None
    )
    boundary = float(gate.get("positive_relative_return_boundary"))
    recommendations = []
    if not directional_ready:
        recommendations.append(
            {
                "type": "continue_forward_tracking",
                "priority": "HIGH",
                "reason": (
                    "directional evidence gate not met: "
                    f"events={distinct_events}/{required_events}, "
                    f"windows={len(available_rows)}/{required_windows}, "
                    f"simulation_available={sim_available}"
                ),
                "affected_config": "position_advisory_v1.yaml",
                "requires_owner_approval": True,
                "manual_review_required": True,
            }
        )
    elif best == "limited_adjustment" and limited_delta is not None and limited_delta > boundary:
        recommendations.append(
            {
                "type": "keep_current_rules",
                "priority": "MEDIUM",
                "reason": "limited_adjustment ranks best and is positive versus no_trade",
                "affected_config": "position_advisory_v1.yaml",
                "requires_owner_approval": True,
                "manual_review_required": True,
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
                "manual_review_required": True,
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
                "manual_review_required": True,
            }
        )
    recommendations.append(
        {
            "type": "continue_forward_tracking",
            "priority": "LOW",
            "reason": "FORWARD_OUTCOME remains higher confidence than backfilled replay",
            "affected_config": "paper_portfolio_v1.yaml",
            "requires_owner_approval": True,
            "manual_review_required": True,
        }
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_calibration_recommendations",
        "recommendations": recommendations,
        "policy_id": metadata.get("policy_id"),
        "policy_version": metadata.get("version"),
        "comparison_window_trading_days": comparison_window,
        "directional_evidence_ready": directional_ready,
        "distinct_replay_event_count": distinct_events,
        "available_comparison_window_count": len(available_rows),
        "minimum_distinct_replay_event_count": required_events,
        "minimum_available_variant_window_count": required_windows,
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
            raise DynamicV3HistoricalReplayError(f"owner review journal time is invalid: {path}")
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
            {
                "variant": variant.get("variant"),
                "turnover": variant.get("turnover"),
                "turnover_convention": variant.get("turnover_convention"),
                "source_status": variant.get("source_status"),
            }
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


def _variant(
    name: str,
    weights: Mapping[str, Any],
    description: str,
    source_status: str,
) -> dict[str, Any]:
    clean = _normalize_weights(weights)
    return {
        "variant": name,
        "description": description,
        "source_status": source_status,
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
    cost_rate: float,
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
        "gross_return": None,
        "estimated_cost": None,
        "return": None,
        "relative_to_no_trade": None,
        "relative_to_consensus_target": None,
        "relative_to_limited_adjustment": None,
        "max_drawdown": None,
        "realized_volatility": None,
        "turnover": turnover,
        "turnover_convention": "one_way_l1_weight_change",
        "cost_rate": cost_rate,
        "cost_role": "initial_turnover_deduction_from_return",
        "risk_metric_cost_role": "gross_price_path_cost_not_applied",
        "outcome_status": "PENDING",
        "outcome_reason": "future_window_not_reached",
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
        "return": None,
        "max_drawdown": None,
        "realized_volatility": None,
        "status": "INSUFFICIENT_DATA",
    }


def _backfill_cost_rate(config: Mapping[str, Any]) -> float:
    simulation = _mapping(config.get("simulation"))
    transaction_cost_bps = _float(simulation.get("transaction_cost_bps"))
    slippage_bps = _float(simulation.get("slippage_bps"))
    if (
        not math.isfinite(transaction_cost_bps)
        or not math.isfinite(slippage_bps)
        or transaction_cost_bps < 0
        or slippage_bps < 0
    ):
        raise DynamicV3HistoricalReplayError(
            "backfill transaction_cost_bps and slippage_bps must be finite and non-negative"
        )
    return round((transaction_cost_bps + slippage_bps) / 10_000.0, 8)


def _run_cached_data_quality_gate(
    *, as_of: date, prices_path: Path, rates_path: Path, report_path: Path
) -> Any:
    quality = _validate_cached_data_quality(
        as_of=as_of,
        prices_path=prices_path,
        rates_path=rates_path,
    )
    write_data_quality_report(quality, report_path)
    return quality


def _validate_cached_data_quality(*, as_of: date, prices_path: Path, rates_path: Path) -> Any:
    universe = load_universe()
    return validate_data_cache(
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


def _frozen_replay_price_rows(
    prices: pd.DataFrame,
    *,
    generated_date: date,
) -> list[dict[str, Any]]:
    return [
        {
            "symbol": _text(row.get("symbol")),
            "date": row["_date"].isoformat(),
            "adj_close": float(row["_adj_close"]),
        }
        for row in prices[["symbol", "_date", "_adj_close"]].to_dict(orient="records")
        if row["_date"] <= generated_date
    ]


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
    start_prices = {
        symbol: _portfolio_price(pivot, start_idx, symbol) for symbol in clean if symbol != "CASH"
    }
    values = []
    for path_date in path_dates:
        portfolio_value = _float(clean.get("CASH"))
        for symbol, weight in clean.items():
            if symbol == "CASH":
                continue
            portfolio_value += _float(weight) * (
                _portfolio_price(pivot, path_date, symbol) / start_prices[symbol]
            )
        values.append(portfolio_value)
    if not values or not math.isfinite(values[-1]) or values[-1] <= 0:
        raise DynamicV3HistoricalReplayError("invalid fixed-share portfolio value path")
    daily_returns = [
        right / left - 1.0 for left, right in zip(values, values[1:], strict=False) if left > 0
    ]
    return values[-1] - 1.0, daily_returns


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
        "total_return": None,
        "annualized_return": None,
        "max_drawdown": None,
        "realized_volatility": None,
        "daily_return_observation_count": 0,
        "turnover": 0.0,
        "estimated_cost": 0.0,
        "trade_count": 0,
        "relative_to_no_trade": None,
        "relative_to_baseline": None,
        "simulation_status": "INSUFFICIENT_DATA",
        "simulation_reason": "no_replay_events",
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


def _finite_window_values(rows: Sequence[Mapping[str, Any]], window: int, key: str) -> list[float]:
    return [
        float(row[key])
        for row in rows
        if _int(row.get("window_days")) == window
        and isinstance(row.get(key), (int, float))
        and not isinstance(row.get(key), bool)
        and math.isfinite(float(row[key]))
    ]


def _rounded_avg_or_none(values: Sequence[float]) -> float | None:
    return round(sum(values) / len(values), 6) if values else None


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
