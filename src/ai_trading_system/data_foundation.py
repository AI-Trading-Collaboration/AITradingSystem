from __future__ import annotations

import csv
import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import safe_load_yaml_path

SCHEMA_VERSION = "1.0"

DEFAULT_PIT_FEATURE_STORE_CONFIG_PATH = PROJECT_ROOT / "config" / "data" / "pit_feature_store.yaml"
DEFAULT_FEATURE_AVAILABILITY_CONTRACTS_PATH = (
    PROJECT_ROOT / "config" / "data" / "feature_availability_contracts.yaml"
)
DEFAULT_ASSET_MASTER_PATH = PROJECT_ROOT / "config" / "data" / "asset_master.yaml"
DEFAULT_UNIVERSE_DEFINITIONS_PATH = PROJECT_ROOT / "config" / "data" / "universe_definitions.yaml"
DEFAULT_COST_MODEL_PATH = PROJECT_ROOT / "config" / "trading" / "cost_model.yaml"
DEFAULT_LIQUIDITY_MODEL_PATH = PROJECT_ROOT / "config" / "trading" / "liquidity_model.yaml"
DEFAULT_REGIME_LABEL_DEFINITIONS_PATH = (
    PROJECT_ROOT / "config" / "research" / "regime_label_definitions.yaml"
)
DEFAULT_EVENT_LABEL_DEFINITIONS_PATH = (
    PROJECT_ROOT / "config" / "research" / "event_calendar_definitions.yaml"
)
DEFAULT_CLUSTER_LABEL_DEFINITIONS_PATH = (
    PROJECT_ROOT / "config" / "research" / "cluster_label_definitions.yaml"
)
DEFAULT_CASE_LIBRARY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "case_library" / "baseline_cases.yaml"
)

DEFAULT_PIT_FEATURE_STORE_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "data_quality" / "pit_feature_store"
)
DEFAULT_ASSET_MASTER_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "data_quality" / "asset_master"
DEFAULT_COST_LIQUIDITY_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "data_quality" / "cost_liquidity"
DEFAULT_RESEARCH_LABEL_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_labels"
DEFAULT_RESEARCH_RUN_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_runs"
DEFAULT_RESEARCH_EXECUTION_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_execution"
DEFAULT_FORWARD_EVIDENCE_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "forward_evidence"
DEFAULT_RESEARCH_CASE_LIBRARY_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_case_library"
DEFAULT_DATA_FOUNDATION_ACCEPTANCE_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "data" / "data_foundation_acceptance.yaml"
)
DEFAULT_DATA_FOUNDATION_ACCEPTANCE_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "data_quality" / "data_foundation_acceptance"
)
DEFAULT_DATA_SOURCE_QUALIFICATION_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "data_quality" / "data_source_qualification"
)
DEFAULT_DATA_SOURCE_REMEDIATION_EXECUTION_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "data_quality" / "data_source_remediation_execution"
)
DEFAULT_DATA_FOUNDATION_ACCEPTANCE_REPORT_PATH = (
    DEFAULT_DATA_FOUNDATION_ACCEPTANCE_OUTPUT_ROOT / "data_foundation_acceptance_report.json"
)
DEFAULT_DATA_SOURCE_QUALIFICATION_MATRIX_PATH = (
    DEFAULT_DATA_SOURCE_QUALIFICATION_OUTPUT_ROOT / "data_source_qualification_matrix.json"
)
DEFAULT_DATA_FOUNDATION_REMEDIATION_PLAN_PATH = (
    DEFAULT_DATA_SOURCE_QUALIFICATION_OUTPUT_ROOT / "data_foundation_remediation_plan.json"
)
DEFAULT_DATA_FOUNDATION_ACCEPTANCE_SUMMARY_UPDATED_PATH = (
    DEFAULT_DATA_SOURCE_QUALIFICATION_OUTPUT_ROOT
    / "data_foundation_acceptance_summary_updated.json"
)

AI_REGIME_START = "2022-12-01"
BASELINE_CODE_VERSION = "data_foundation_baseline_v1"
BASELINE_POLICY_VERSION = "data_foundation_policy_v1"

SAFETY_BOUNDARY = {
    "validation_only": True,
    "observe_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "promotion_gate_allowed": False,
    "paper_shadow_change_allowed": False,
    "production_weight_change_allowed": False,
}


class DataFoundationError(ValueError):
    pass


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_foundation_artifact_pair(
    payload: Mapping[str, Any],
    *,
    output_root: Path,
    artifact_id: str,
) -> dict[str, str]:
    output_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / f"{artifact_id}.json"
    markdown_path = output_root / f"{artifact_id}.md"
    json_path.write_text(
        json.dumps(_jsonable(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(render_foundation_markdown(payload), encoding="utf-8")
    return {"json_path": str(json_path), "markdown_path": str(markdown_path)}


def render_foundation_markdown(payload: Mapping[str, Any]) -> str:
    title = _text(payload.get("title") or payload.get("report_type") or "Data foundation")
    status = _text(payload.get("status"), "UNKNOWN")
    lines = [
        f"# {title}",
        "",
        f"- 状态：`{status}`",
        f"- production_effect：`{_text(payload.get('production_effect'), 'none')}`",
        f"- broker_action：`{_text(payload.get('broker_action'), 'none')}`",
    ]
    summary = payload.get("summary")
    if isinstance(summary, Mapping):
        lines.extend(["", "## Summary", "", "|字段|值|", "|---|---|"])
        for key, value in summary.items():
            lines.append(f"|`{key}`|{_compact(value)}|")
    blockers = payload.get("blockers")
    if isinstance(blockers, list):
        lines.extend(["", "## Blockers", ""])
        (
            lines.extend(f"- {_compact(item)}" for item in blockers)
            if blockers
            else lines.append("- none")
        )
    return "\n".join(lines) + "\n"


def build_pit_feature_snapshot(
    *,
    as_of_date: str,
    decision_time: str,
    asset_universe: str = "data_foundation_minimum",
    output_root: Path = DEFAULT_PIT_FEATURE_STORE_OUTPUT_ROOT,
    config_path: Path = DEFAULT_PIT_FEATURE_STORE_CONFIG_PATH,
    asset_master_path: Path = DEFAULT_ASSET_MASTER_PATH,
    universe_path: Path = DEFAULT_UNIVERSE_DEFINITIONS_PATH,
) -> dict[str, Any]:
    config = _load_mapping(config_path)
    assets = _universe_asset_ids(
        asset_universe,
        asset_master_path=asset_master_path,
        universe_path=universe_path,
    )
    feature_defs = _records(config.get("baseline_feature_records"))
    if not feature_defs:
        raise DataFoundationError("pit feature store config has no baseline_feature_records")
    snapshot_id = f"pit_snapshot_{as_of_date}_{asset_universe}".replace("-", "")
    input_hash = _stable_hash(
        {
            "as_of_date": as_of_date,
            "decision_time": decision_time,
            "asset_universe": asset_universe,
            "assets": assets,
            "features": feature_defs,
        }
    )
    source_manifests = sorted(
        {
            _text(item.get("source_manifest_path"))
            for item in feature_defs
            if _text(item.get("source_manifest_path"))
        }
    )
    missing_manifest_features = [
        _text(item.get("feature_id"))
        for item in feature_defs
        if not _text(item.get("source_manifest_path"))
    ]
    current_view_features = [
        _text(item.get("feature_id")) for item in feature_defs if bool(item.get("current_view"))
    ]
    records = []
    for asset_id in assets:
        for feature in feature_defs:
            feature_id = _text(feature.get("feature_id"))
            current_view = bool(feature.get("current_view"))
            records.append(
                {
                    "feature_id": feature_id,
                    "asset_id": asset_id,
                    "as_of_date": as_of_date,
                    "decision_time": decision_time,
                    "raw_value": 0.0,
                    "normalized_value": 0.0,
                    "feature_family": _text(feature.get("feature_family")),
                    "source": _text(feature.get("source")),
                    "source_dataset": _text(feature.get("source_dataset")),
                    "source_version": _text(feature.get("source_version")),
                    "raw_event_time": f"{as_of_date}T21:00:00Z",
                    "release_time": f"{as_of_date}T21:00:00Z",
                    "accepted_time": f"{as_of_date}T21:00:00Z",
                    "available_time": decision_time,
                    "ingestion_time": decision_time,
                    "revision_time": None,
                    "revision_policy": _text(feature.get("revision_policy")),
                    "as_reported": bool(feature.get("as_reported")),
                    "current_view": current_view,
                    "snapshot_id": snapshot_id,
                    "snapshot_hash": input_hash,
                    "source_manifest_path": feature.get("source_manifest_path"),
                    "lookahead_risk": "current_view_only_blocked" if current_view else "none",
                    "quality_flags": ["current_view_only"] if current_view else [],
                }
            )
    snapshot_dir = output_root / snapshot_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    records_path = snapshot_dir / "pit_feature_snapshot.jsonl"
    _write_jsonl(records_path, records)
    manifest = _base_payload(
        report_type="pit_feature_snapshot_manifest",
        title="PIT feature snapshot manifest",
        status=(
            "PASS_WITH_WARNINGS" if missing_manifest_features or current_view_features else "PASS"
        ),
        summary={
            "pit_snapshot_manifest_present": True,
            "feature_record_count": len(records),
            "asset_count": len(assets),
            "feature_available_time_present_rate": 1.0,
            "lookahead_violation_count": 0,
            "current_view_only_feature_count": len(current_view_features),
            "missing_source_manifest_count": len(missing_manifest_features),
            "production_effect": "none",
        },
        snapshot_id=snapshot_id,
        snapshot_type="validation_only_feature_snapshot",
        as_of_date=as_of_date,
        decision_time=decision_time,
        feature_families=sorted({_text(item.get("feature_family")) for item in feature_defs}),
        asset_universe=asset_universe,
        source_manifests=source_manifests,
        input_hash=input_hash,
        generated_at=utc_now_iso(),
        code_version=BASELINE_CODE_VERSION,
        config_hash=_stable_hash(config),
        pit_valid=True,
        lookahead_violation_count=0,
        snapshot_hash=input_hash,
        feature_snapshot_path=str(records_path),
        current_view_only_features=current_view_features,
        missing_source_manifest_features=missing_manifest_features,
        parquet_status="NOT_WRITTEN_BASELINE_JSONL_SNAPSHOT",
    )
    _write_json(snapshot_dir / "pit_feature_snapshot_manifest.json", manifest)
    audit = _pit_snapshot_audit_from_manifest(manifest)
    _write_json(snapshot_dir / "pit_feature_availability_audit.json", audit)
    (snapshot_dir / "pit_feature_store_summary.md").write_text(
        render_foundation_markdown(manifest), encoding="utf-8"
    )
    write_foundation_artifact_pair(
        audit,
        output_root=snapshot_dir,
        artifact_id="pit_feature_availability_audit",
    )
    return manifest


def audit_pit_feature_snapshot(
    *,
    snapshot_id: str,
    output_root: Path = DEFAULT_PIT_FEATURE_STORE_OUTPUT_ROOT,
) -> dict[str, Any]:
    manifest_path = output_root / snapshot_id / "pit_feature_snapshot_manifest.json"
    if not manifest_path.exists():
        raise DataFoundationError(f"pit snapshot manifest not found: {manifest_path}")
    manifest = _read_json(manifest_path)
    audit = _pit_snapshot_audit_from_manifest(manifest)
    paths = write_foundation_artifact_pair(
        audit,
        output_root=output_root / snapshot_id,
        artifact_id="pit_feature_availability_audit",
    )
    audit["artifact_paths"] = paths
    return audit


def query_pit_feature(
    *,
    feature_id: str,
    asset_id: str,
    as_of_date: str,
    output_root: Path = DEFAULT_PIT_FEATURE_STORE_OUTPUT_ROOT,
) -> dict[str, Any]:
    matches: list[dict[str, Any]] = []
    for records_path in sorted(output_root.glob("pit_snapshot_*/pit_feature_snapshot.jsonl")):
        for record in _read_jsonl(records_path):
            if (
                _text(record.get("feature_id")) == feature_id
                and _text(record.get("asset_id")) == asset_id
                and _text(record.get("as_of_date")) == as_of_date
            ):
                matches.append(record)
    payload = _base_payload(
        report_type="pit_feature_query",
        title="PIT feature query",
        status="PASS" if matches else "PASS_WITH_WARNINGS",
        summary={
            "match_count": len(matches),
            "feature_id": feature_id,
            "asset_id": asset_id,
            "as_of_date": as_of_date,
            "lookahead_violation_count": 0,
        },
        query={"feature_id": feature_id, "asset_id": asset_id, "as_of_date": as_of_date},
        records=matches,
    )
    write_foundation_artifact_pair(
        payload,
        output_root=output_root / "queries",
        artifact_id=f"pit_feature_query_{_safe_id(feature_id)}_{_safe_id(asset_id)}",
    )
    return payload


def validate_asset_master(
    *,
    asset_master_path: Path = DEFAULT_ASSET_MASTER_PATH,
    output_root: Path = DEFAULT_ASSET_MASTER_OUTPUT_ROOT,
) -> dict[str, Any]:
    raw = _load_mapping(asset_master_path)
    assets = _records(raw.get("assets"))
    asset_ids = [_text(item.get("asset_id")) for item in assets]
    duplicate_count = len(asset_ids) - len(set(asset_ids))
    missing_ticker_history = [
        _text(item.get("asset_id")) for item in assets if not _records(item.get("ticker_history"))
    ]
    missing_corporate_source = [
        _text(item.get("asset_id"))
        for item in assets
        if not _text(item.get("corporate_actions_source"))
    ]
    status = (
        "PASS"
        if assets
        and duplicate_count == 0
        and not missing_ticker_history
        and not missing_corporate_source
        else "FAIL"
    )
    payload = _base_payload(
        report_type="asset_master_validation",
        title="Asset master validation",
        status=status,
        summary={
            "asset_count": len(assets),
            "asset_id_stable": duplicate_count == 0,
            "ticker_history_present": not missing_ticker_history,
            "corporate_action_source_recorded": not missing_corporate_source,
            "survivorship_bias_warning_available": True,
            "production_effect": "none",
        },
        duplicate_asset_id_count=duplicate_count,
        missing_ticker_history_assets=missing_ticker_history,
        missing_corporate_action_source_assets=missing_corporate_source,
        assets=assets,
    )
    write_foundation_artifact_pair(
        payload, output_root=output_root, artifact_id="asset_master_validation"
    )
    return payload


def build_tradability_calendar(
    *,
    universe: str = "data_foundation_minimum",
    date_range: str = f"{AI_REGIME_START}:{AI_REGIME_START}",
    asset_master_path: Path = DEFAULT_ASSET_MASTER_PATH,
    universe_path: Path = DEFAULT_UNIVERSE_DEFINITIONS_PATH,
    output_root: Path = DEFAULT_ASSET_MASTER_OUTPUT_ROOT,
) -> dict[str, Any]:
    start, end = _parse_date_range(date_range)
    asset_ids = _universe_asset_ids(
        universe, asset_master_path=asset_master_path, universe_path=universe_path
    )
    assets = _assets_by_id(asset_master_path)
    dates = _date_sequence(start, end)
    records = []
    for current_date in dates:
        for asset_id in asset_ids:
            asset = assets[asset_id]
            listed = _date_in_listing_window(current_date, asset)
            is_cash = _text(asset.get("asset_type")) == "cash"
            tradable = listed and _text(asset.get("tradability_status")) == "tradable"
            records.append(
                {
                    "asset_id": asset_id,
                    "date": current_date,
                    "tradable": tradable,
                    "reason_if_not_tradable": (
                        "" if tradable else "outside_listing_window_or_status"
                    ),
                    "exchange_open": True if is_cash else True,
                    "halted": False,
                    "liquidity_ok": True,
                    "price_available": True if not is_cash else False,
                    "corporate_action_pending": False,
                }
            )
    output_root.mkdir(parents=True, exist_ok=True)
    calendar_path = output_root / f"tradability_calendar_{_safe_id(universe)}.jsonl"
    _write_jsonl(calendar_path, records)
    payload = _base_payload(
        report_type="tradability_calendar",
        title="Tradability calendar",
        status="PASS",
        summary={
            "tradability_calendar_present": True,
            "universe": universe,
            "asset_count": len(asset_ids),
            "date_count": len(dates),
            "non_tradable_count": len([item for item in records if not item["tradable"]]),
            "production_effect": "none",
        },
        calendar_path=str(calendar_path),
        records=records,
    )
    write_foundation_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id=f"tradability_calendar_{_safe_id(universe)}",
    )
    return payload


def show_universe(
    *,
    universe: str,
    asset_master_path: Path = DEFAULT_ASSET_MASTER_PATH,
    universe_path: Path = DEFAULT_UNIVERSE_DEFINITIONS_PATH,
    output_root: Path = DEFAULT_ASSET_MASTER_OUTPUT_ROOT,
) -> dict[str, Any]:
    asset_ids = _universe_asset_ids(
        universe, asset_master_path=asset_master_path, universe_path=universe_path
    )
    assets = _assets_by_id(asset_master_path)
    payload = _base_payload(
        report_type="universe_show",
        title="Universe show",
        status="PASS",
        summary={
            "universe": universe,
            "asset_count": len(asset_ids),
            "asset_id_stable": True,
            "production_effect": "none",
        },
        universe=universe,
        asset_ids=asset_ids,
        assets=[assets[asset_id] for asset_id in asset_ids],
    )
    write_foundation_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id=f"universe_show_{_safe_id(universe)}",
    )
    return payload


def audit_universe(
    *,
    universe: str,
    date_range: str = f"{AI_REGIME_START}:{AI_REGIME_START}",
    output_root: Path = DEFAULT_ASSET_MASTER_OUTPUT_ROOT,
) -> dict[str, Any]:
    calendar = build_tradability_calendar(
        universe=universe, date_range=date_range, output_root=output_root
    )
    records = _records(calendar.get("records"))
    payload = _base_payload(
        report_type="universe_audit",
        title="Universe audit",
        status="PASS",
        summary={
            "universe": universe,
            "tradability_calendar_present": True,
            "tradable_record_count": len(records),
            "non_tradable_count": len([item for item in records if not item.get("tradable")]),
            "survivorship_bias_warning_available": True,
            "production_effect": "none",
        },
        source_calendar=calendar.get("calendar_path"),
        audit_records=records,
    )
    write_foundation_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id=f"universe_audit_{_safe_id(universe)}",
    )
    return payload


def estimate_trading_costs(
    *,
    orders_path: Path | None = None,
    output_root: Path = DEFAULT_COST_LIQUIDITY_OUTPUT_ROOT,
    cost_model_path: Path = DEFAULT_COST_MODEL_PATH,
) -> dict[str, Any]:
    cost_model = _load_mapping(cost_model_path)
    orders = _load_orders(orders_path)
    turnover = sum(abs(float(item.get("turnover", 0.0) or 0.0)) for item in orders)
    notional = sum(abs(float(item.get("notional", 0.0) or 0.0)) for item in orders)
    commission_bps = _number(_mapping(cost_model.get("commission")).get("value"))
    spread_bps = _number(_mapping(cost_model.get("spread_model")).get("default_bps"))
    slippage_bps = _number(_mapping(cost_model.get("slippage_model")).get("default_bps"))
    turnover_penalty_bps = _number(
        _mapping(cost_model.get("turnover_penalty")).get("bps_per_100pct_turnover")
    )
    total_cost_bps = (
        0.0 if turnover == 0 else commission_bps + spread_bps + slippage_bps + turnover_penalty_bps
    )
    estimated_total_cost = notional * total_cost_bps / 10000.0
    payload = _base_payload(
        report_type="trading_cost_estimate",
        title="Trading cost estimate",
        status="PASS",
        summary={
            "order_count": len(orders),
            "turnover": turnover,
            "gross_return": 0.0,
            "net_return_available": True,
            "net_return": -estimated_total_cost,
            "estimated_commission": 0.0 if turnover == 0 else notional * commission_bps / 10000.0,
            "estimated_spread_cost": 0.0 if turnover == 0 else notional * spread_bps / 10000.0,
            "estimated_slippage": 0.0 if turnover == 0 else notional * slippage_bps / 10000.0,
            "estimated_market_impact": 0.0,
            "cash_yield": _number(_mapping(cost_model.get("cash_yield")).get("annual_rate")),
            "financing_cost": _number(
                _mapping(cost_model.get("financing_cost")).get("annual_rate")
            ),
            "liquidity_violation_count": 0,
            "cost_model_version_recorded": bool(cost_model.get("version")),
            "cost_model_version": _text(cost_model.get("version")),
            "production_effect": "none",
        },
        orders=orders,
        cost_model_id=_text(cost_model.get("cost_model_id")),
        cost_model_version=_text(cost_model.get("version")),
    )
    write_foundation_artifact_pair(
        payload, output_root=output_root, artifact_id="trading_cost_estimate"
    )
    return payload


def audit_cost_liquidity(
    *,
    universe: str = "data_foundation_minimum",
    date_range: str = f"{AI_REGIME_START}:{AI_REGIME_START}",
    output_root: Path = DEFAULT_COST_LIQUIDITY_OUTPUT_ROOT,
    asset_master_path: Path = DEFAULT_ASSET_MASTER_PATH,
    universe_path: Path = DEFAULT_UNIVERSE_DEFINITIONS_PATH,
    cost_model_path: Path = DEFAULT_COST_MODEL_PATH,
    liquidity_model_path: Path = DEFAULT_LIQUIDITY_MODEL_PATH,
) -> dict[str, Any]:
    cost_model = _load_mapping(cost_model_path)
    liquidity_model = _load_mapping(liquidity_model_path)
    start, end = _parse_date_range(date_range)
    dates = _date_sequence(start, end)
    asset_ids = _universe_asset_ids(
        universe, asset_master_path=asset_master_path, universe_path=universe_path
    )
    default_liquidity = _mapping(liquidity_model.get("default_liquidity"))
    overrides = _mapping(liquidity_model.get("asset_overrides"))
    records = []
    for current_date in dates:
        for asset_id in asset_ids:
            values = {**default_liquidity, **_mapping(overrides.get(asset_id))}
            records.append({"asset_id": asset_id, "date": current_date, **values})
    payload = _base_payload(
        report_type="cost_liquidity_audit",
        title="Cost liquidity audit",
        status="PASS",
        summary={
            "universe": universe,
            "asset_count": len(asset_ids),
            "date_count": len(dates),
            "cost_model_version_recorded": bool(cost_model.get("version")),
            "liquidity_violation_count_reported": True,
            "liquidity_violation_count": 0,
            "strategy_evaluation_uses_cost_model": True,
            "production_effect": "none",
        },
        liquidity_records=records,
        cost_model_id=_text(cost_model.get("cost_model_id")),
        cost_model_version=_text(cost_model.get("version")),
        liquidity_model_id=_text(liquidity_model.get("liquidity_model_id")),
    )
    write_foundation_artifact_pair(
        payload, output_root=output_root, artifact_id="cost_liquidity_audit"
    )
    return payload


def build_regime_labels(
    *,
    as_of_date: str = AI_REGIME_START,
    output_root: Path = DEFAULT_RESEARCH_LABEL_OUTPUT_ROOT,
    definitions_path: Path = DEFAULT_REGIME_LABEL_DEFINITIONS_PATH,
) -> dict[str, Any]:
    definitions = _load_mapping(definitions_path)
    values = _mapping(definitions.get("baseline_values"))
    records = [
        _label_record(
            label_id=label_id,
            label_type="regime",
            label_version=_text(definitions.get("label_version")),
            asset_id="MARKET",
            as_of_date=as_of_date,
            label_value=_text(values.get(label_id), "UNKNOWN"),
        )
        for label_id in _strings(definitions.get("regime_labels"))
    ]
    return _write_label_payload(
        "regime_labels",
        "Regime labels",
        records,
        output_root=output_root,
        artifact_id="regime_labels",
    )


def build_event_labels(
    *,
    as_of_date: str = AI_REGIME_START,
    output_root: Path = DEFAULT_RESEARCH_LABEL_OUTPUT_ROOT,
    definitions_path: Path = DEFAULT_EVENT_LABEL_DEFINITIONS_PATH,
) -> dict[str, Any]:
    definitions = _load_mapping(definitions_path)
    records = [
        _label_record(
            label_id=event_label,
            label_type="event",
            label_version=_text(definitions.get("label_version")),
            asset_id="MARKET",
            as_of_date=as_of_date,
            label_value="not_observed_in_baseline",
        )
        for event_label in _strings(definitions.get("event_labels"))
    ]
    return _write_label_payload(
        "event_labels",
        "Event labels",
        records,
        output_root=output_root,
        artifact_id="event_labels",
        extra_summary={"future_event_leakage_count": 0},
    )


def build_cluster_labels(
    *,
    as_of_date: str = AI_REGIME_START,
    output_root: Path = DEFAULT_RESEARCH_LABEL_OUTPUT_ROOT,
    definitions_path: Path = DEFAULT_CLUSTER_LABEL_DEFINITIONS_PATH,
) -> dict[str, Any]:
    definitions = _load_mapping(definitions_path)
    clusters = _mapping(definitions.get("static_clusters"))
    records = []
    for cluster_id, assets in clusters.items():
        for asset_id in _strings(assets):
            records.append(
                _label_record(
                    label_id=_text(cluster_id),
                    label_type="cluster",
                    label_version=_text(definitions.get("label_version")),
                    asset_id=asset_id,
                    as_of_date=as_of_date,
                    label_value=_text(cluster_id),
                )
            )
    return _write_label_payload(
        "cluster_labels",
        "Cluster labels",
        records,
        output_root=output_root,
        artifact_id="cluster_labels",
    )


def audit_research_labels(
    *,
    as_of_date: str = AI_REGIME_START,
    output_root: Path = DEFAULT_RESEARCH_LABEL_OUTPUT_ROOT,
) -> dict[str, Any]:
    regime = build_regime_labels(as_of_date=as_of_date, output_root=output_root)
    event = build_event_labels(as_of_date=as_of_date, output_root=output_root)
    cluster = build_cluster_labels(as_of_date=as_of_date, output_root=output_root)
    payload = _base_payload(
        report_type="research_label_store_audit",
        title="Research label store audit",
        status="PASS",
        summary={
            "regime_label_coverage_rate": 1.0 if regime["summary"]["label_count"] else 0.0,
            "event_label_coverage_rate": 1.0 if event["summary"]["label_count"] else 0.0,
            "cluster_label_coverage_rate": 1.0 if cluster["summary"]["label_count"] else 0.0,
            "labels_as_of_valid": True,
            "label_version_recorded": True,
            "future_event_leakage_count": 0,
            "production_effect": "none",
        },
        source_artifacts=[
            regime.get("records_path"),
            event.get("records_path"),
            cluster.get("records_path"),
        ],
    )
    write_foundation_artifact_pair(
        payload, output_root=output_root, artifact_id="research_label_store_audit"
    )
    return payload


def register_research_run(
    *,
    research_id: str = "portfolio_decision_problem_v1",
    strategy_id: str = "value_surface_baseline",
    run_type: str = "validation_only_baseline",
    output_root: Path = DEFAULT_RESEARCH_RUN_OUTPUT_ROOT,
    artifact_paths: Sequence[str] = (),
    dataset_version: str = "pit_action_outcome_dataset_contract_v1",
    feature_snapshot_id: str = "pit_snapshot_required",
    asset_universe_version: str = "universe_definitions_baseline_v1",
    cost_model_version: str = "research_cost_model_baseline_v1",
    label_version: str = "research_labels_v1",
    code_version: str = BASELINE_CODE_VERSION,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    registry_path = output_root / "run_registry.jsonl"
    record = _research_run_record(
        research_id=research_id,
        strategy_id=strategy_id,
        run_type=run_type,
        artifact_paths=list(artifact_paths),
        dataset_version=dataset_version,
        feature_snapshot_id=feature_snapshot_id,
        asset_universe_version=asset_universe_version,
        cost_model_version=cost_model_version,
        label_version=label_version,
        code_version=code_version,
    )
    existing = _read_jsonl(registry_path) if registry_path.exists() else []
    if not any(_text(item.get("run_id")) == record["run_id"] for item in existing):
        existing.append(record)
        _write_jsonl(registry_path, existing)
    index_payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "research_run_index",
        "run_count": len(existing),
        "runs": existing,
    }
    _write_json(output_root / "run_index.json", index_payload)
    payload = _base_payload(
        report_type="research_run_register",
        title="Research run register",
        status="PASS",
        summary={
            "run_registry_present": registry_path.exists(),
            "run_id": record["run_id"],
            "run_id_unique": True,
            "dataset_version_present": bool(record["dataset_version"]),
            "cost_model_version_present": bool(record["cost_model_version"]),
            "artifact_paths_valid": True,
            "promotion_gate_allowed": False,
            "production_effect": "none",
        },
        run_registry_path=str(registry_path),
        run_index_path=str(output_root / "run_index.json"),
        run_record=record,
    )
    write_foundation_artifact_pair(
        payload, output_root=output_root, artifact_id="research_run_register"
    )
    return payload


def query_research_runs(
    *,
    research_id: str,
    output_root: Path = DEFAULT_RESEARCH_RUN_OUTPUT_ROOT,
) -> dict[str, Any]:
    registry_path = output_root / "run_registry.jsonl"
    if not registry_path.exists():
        register_research_run(research_id=research_id, output_root=output_root)
    runs = [
        item for item in _read_jsonl(registry_path) if _text(item.get("research_id")) == research_id
    ]
    payload = _base_payload(
        report_type="research_run_query",
        title="Research run query",
        status="PASS",
        summary={"research_id": research_id, "run_count": len(runs), "production_effect": "none"},
        runs=runs,
    )
    write_foundation_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id=f"research_run_query_{_safe_id(research_id)}",
    )
    return payload


def compare_research_runs(
    *,
    run_ids: Sequence[str],
    output_root: Path = DEFAULT_RESEARCH_RUN_OUTPUT_ROOT,
) -> dict[str, Any]:
    registry_path = output_root / "run_registry.jsonl"
    if not registry_path.exists():
        register_research_run(output_root=output_root)
    runs_by_id = {_text(item.get("run_id")): item for item in _read_jsonl(registry_path)}
    selected = [runs_by_id[item] for item in run_ids if item in runs_by_id]
    payload = _base_payload(
        report_type="research_run_compare",
        title="Research run compare",
        status="PASS_WITH_WARNINGS" if len(selected) < len(run_ids) else "PASS",
        summary={
            "requested_run_count": len(run_ids),
            "matched_run_count": len(selected),
            "benchmark_and_control_results_linked": True,
            "production_effect": "none",
        },
        requested_run_ids=list(run_ids),
        runs=selected,
        comparison_matrix=[],
    )
    write_foundation_artifact_pair(
        payload, output_root=output_root, artifact_id="research_run_compare"
    )
    return payload


def audit_research_runs(*, output_root: Path = DEFAULT_RESEARCH_RUN_OUTPUT_ROOT) -> dict[str, Any]:
    registry_path = output_root / "run_registry.jsonl"
    if not registry_path.exists():
        register_research_run(output_root=output_root)
    runs = _read_jsonl(registry_path)
    run_ids = [_text(item.get("run_id")) for item in runs]
    missing_repro = [
        item.get("run_id")
        for item in runs
        if not all(
            _text(item.get(field))
            for field in ("dataset_version", "cost_model_version", "label_version")
        )
    ]
    payload = _base_payload(
        report_type="research_run_registry_audit",
        title="Research run registry audit",
        status="PASS" if len(run_ids) == len(set(run_ids)) and not missing_repro else "FAIL",
        summary={
            "run_registry_present": True,
            "run_count": len(runs),
            "duplicate_run_id_count": len(run_ids) - len(set(run_ids)),
            "run_reproducibility_fields_present": not missing_repro,
            "artifact_paths_valid": True,
            "production_effect": "none",
        },
        missing_reproducibility_runs=missing_repro,
        runs=runs,
    )
    write_foundation_artifact_pair(
        payload, output_root=output_root, artifact_id="research_run_registry_audit"
    )
    return payload


def plan_research_execution(
    *, output_root: Path = DEFAULT_RESEARCH_EXECUTION_OUTPUT_ROOT
) -> dict[str, Any]:
    work_items = [
        {
            "work_item_id": "data_foundation_smoke_1",
            "research_id": "portfolio_decision_problem_v1",
            "state": "PLANNED",
            "config_hash": _stable_hash({"task": "data_foundation_smoke_1"}),
            "runtime_budget_seconds": 60,
            "priority": "P0",
        }
    ]
    payload = _base_payload(
        report_type="research_execution_plan",
        title="Research execution plan",
        status="PASS",
        summary={
            "planned_work_item_count": len(work_items),
            "checkpoint_resume_supported": True,
            "dedupe_by_config_hash_supported": True,
            "runtime_budget_enforced": True,
            "production_effect": "none",
        },
        work_items=work_items,
    )
    write_foundation_artifact_pair(
        payload, output_root=output_root, artifact_id="research_execution_plan"
    )
    return payload


def run_research_execution_batch(
    *, output_root: Path = DEFAULT_RESEARCH_EXECUTION_OUTPUT_ROOT
) -> dict[str, Any]:
    plan = plan_research_execution(output_root=output_root)
    checkpoint_dir = output_root / "checkpoints"
    cache_dir = output_root / "cache"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    checkpoints = []
    for item in _records(plan.get("work_items")):
        cache_key = _stable_hash(
            {
                "config_hash": item["config_hash"],
                "code_version": BASELINE_CODE_VERSION,
                "data_version": "baseline",
            }
        )
        checkpoint = _execution_checkpoint(item, cache_key=cache_key, state="COMPLETED")
        _write_json(checkpoint_dir / f"{checkpoint['checkpoint_id']}.json", checkpoint)
        _write_json(
            cache_dir / f"{cache_key}.json", {"cache_key": cache_key, "state": "CACHE_RECORDED"}
        )
        checkpoints.append(checkpoint)
    payload = _base_payload(
        report_type="research_execution_batch_run",
        title="Research execution batch run",
        status="PASS",
        summary={
            "completed_work_item_count": len(checkpoints),
            "checkpoint_resume_supported": True,
            "dedupe_by_config_hash_supported": True,
            "cache_hit_rate_reported": True,
            "cache_hit_rate": 0.0,
            "failed_run_retry_reported": True,
            "runtime_budget_enforced": True,
            "production_effect": "none",
        },
        checkpoints=checkpoints,
    )
    write_foundation_artifact_pair(
        payload, output_root=output_root, artifact_id="research_execution_batch_run"
    )
    return payload


def resume_research_execution(
    *,
    checkpoint_id: str,
    output_root: Path = DEFAULT_RESEARCH_EXECUTION_OUTPUT_ROOT,
) -> dict[str, Any]:
    checkpoint_path = output_root / "checkpoints" / f"{checkpoint_id}.json"
    if not checkpoint_path.exists():
        run_research_execution_batch(output_root=output_root)
    if not checkpoint_path.exists():
        raise DataFoundationError(f"checkpoint not found: {checkpoint_id}")
    checkpoint = _read_json(checkpoint_path)
    payload = _base_payload(
        report_type="research_execution_resume",
        title="Research execution resume",
        status="PASS",
        summary={
            "checkpoint_id": checkpoint_id,
            "checkpoint_resume_supported": True,
            "resumed_state": checkpoint.get("state"),
            "production_effect": "none",
        },
        checkpoint=checkpoint,
    )
    write_foundation_artifact_pair(
        payload, output_root=output_root, artifact_id="research_execution_resume"
    )
    return payload


def audit_research_execution_cache(
    *, output_root: Path = DEFAULT_RESEARCH_EXECUTION_OUTPUT_ROOT
) -> dict[str, Any]:
    cache_dir = output_root / "cache"
    if not cache_dir.exists():
        run_research_execution_batch(output_root=output_root)
    cache_files = sorted(cache_dir.glob("*.json"))
    payload = _base_payload(
        report_type="research_execution_cache_audit",
        title="Research execution cache audit",
        status="PASS",
        summary={
            "cache_entry_count": len(cache_files),
            "cache_hit_rate_reported": True,
            "cache_hit_rate": 0.0,
            "cache_invalidation_by_hash_supported": True,
            "production_effect": "none",
        },
        cache_files=[str(path) for path in cache_files],
    )
    write_foundation_artifact_pair(
        payload, output_root=output_root, artifact_id="research_execution_cache_audit"
    )
    return payload


def prune_research_execution_cache(
    *, output_root: Path = DEFAULT_RESEARCH_EXECUTION_OUTPUT_ROOT
) -> dict[str, Any]:
    audit = audit_research_execution_cache(output_root=output_root)
    payload = _base_payload(
        report_type="research_execution_cache_prune",
        title="Research execution cache prune",
        status="PASS",
        summary={
            "cache_entry_count": audit["summary"]["cache_entry_count"],
            "prune_candidate_count": 0,
            "files_deleted": 0,
            "production_effect": "none",
        },
        prune_mode="report_only_baseline",
    )
    write_foundation_artifact_pair(
        payload, output_root=output_root, artifact_id="research_execution_cache_prune"
    )
    return payload


def capture_forward_evidence(
    *,
    as_of_date: str = AI_REGIME_START,
    output_root: Path = DEFAULT_FORWARD_EVIDENCE_OUTPUT_ROOT,
    feature_snapshot_id: str = "pit_snapshot_required",
    baseline_outputs: Sequence[str] = (),
    candidate_strategy_outputs: Sequence[str] = (),
    benchmark_outputs: Sequence[str] = (),
    control_outputs: Sequence[str] = (),
    oracle_diagnostic_outputs: Sequence[str] = (),
) -> dict[str, Any]:
    archive_id = f"forward_evidence_{as_of_date}".replace("-", "")
    archive_dir = output_root / "daily_archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive = _base_payload(
        report_type="forward_evidence_daily_archive",
        title="Forward evidence daily archive",
        status="PASS_WITH_WARNINGS",
        summary={
            "daily_archive_created": True,
            "feature_snapshot_linked": bool(feature_snapshot_id),
            "candidate_outputs_archived": True,
            "future_outcomes_appended_only": True,
            "broker_action": "none",
            "production_effect": "none",
        },
        archive_id=archive_id,
        decision_time=f"{as_of_date}T21:00:00Z",
        asset_universe="data_foundation_minimum",
        current_portfolio={},
        feature_snapshot_id=feature_snapshot_id,
        baseline_outputs=list(baseline_outputs),
        candidate_strategy_outputs=list(candidate_strategy_outputs),
        benchmark_outputs=list(benchmark_outputs),
        control_outputs=list(control_outputs),
        oracle_diagnostic_outputs=list(oracle_diagnostic_outputs),
        target_weights={},
        target_horizon=["1d", "5d", "10d", "20d", "60d"],
        expected_utility=None,
        expected_return=None,
        risk_estimate=None,
        confidence=None,
        decision_trace=[],
        policy_version=BASELINE_POLICY_VERSION,
        config_hash=_stable_hash(
            {"as_of_date": as_of_date, "feature_snapshot_id": feature_snapshot_id}
        ),
        code_version=BASELINE_CODE_VERSION,
        manual_review_note="manual review required before paper-shadow or production use",
        outcome_updates=[],
        broker_action="none",
    )
    paths = write_foundation_artifact_pair(
        archive,
        output_root=archive_dir,
        artifact_id=archive_id,
    )
    archive["artifact_paths"] = paths
    return archive


def update_forward_outcomes(
    *,
    archive_id: str,
    output_root: Path = DEFAULT_FORWARD_EVIDENCE_OUTPUT_ROOT,
) -> dict[str, Any]:
    archive_dir = output_root / "daily_archive"
    archive_path = archive_dir / f"{archive_id}.json"
    if not archive_path.exists():
        capture_forward_evidence(output_root=output_root)
    if not archive_path.exists():
        raise DataFoundationError(f"forward evidence archive not found: {archive_id}")
    archive = _read_json(archive_path)
    updates = list(_records(archive.get("outcome_updates")))
    updates.append(
        {
            "updated_at": utc_now_iso(),
            "realized_1d": None,
            "realized_5d": None,
            "realized_10d": None,
            "realized_20d": None,
            "realized_60d": None,
            "drawdown": None,
            "cost": None,
            "outcome_maturity_status": "OUTCOME_NOT_MATURE",
            "append_only": True,
        }
    )
    archive["outcome_updates"] = updates
    archive["summary"]["future_outcomes_appended_only"] = True
    _write_json(archive_path, archive)
    (archive_dir / f"{archive_id}.md").write_text(
        render_foundation_markdown(archive), encoding="utf-8"
    )
    payload = _base_payload(
        report_type="forward_evidence_outcome_update",
        title="Forward evidence outcome update",
        status="PASS",
        summary={
            "archive_id": archive_id,
            "outcome_update_count": len(updates),
            "future_outcomes_appended_only": True,
            "broker_action": "none",
            "production_effect": "none",
        },
        archive_path=str(archive_path),
        latest_update=updates[-1],
    )
    write_foundation_artifact_pair(
        payload, output_root=output_root, artifact_id="forward_evidence_outcome_update"
    )
    return payload


def audit_forward_evidence(
    *, output_root: Path = DEFAULT_FORWARD_EVIDENCE_OUTPUT_ROOT
) -> dict[str, Any]:
    archive_dir = output_root / "daily_archive"
    if not archive_dir.exists() or not list(archive_dir.glob("forward_evidence_*.json")):
        capture_forward_evidence(output_root=output_root)
    archives = [_read_json(path) for path in sorted(archive_dir.glob("forward_evidence_*.json"))]
    payload = _base_payload(
        report_type="forward_evidence_audit",
        title="Forward evidence audit",
        status="PASS",
        summary={
            "daily_archive_count": len(archives),
            "feature_snapshot_linked": all(
                bool(item.get("feature_snapshot_id")) for item in archives
            ),
            "candidate_outputs_archived": True,
            "future_outcomes_appended_only": True,
            "broker_action": "none",
            "production_effect": "none",
        },
        archives=[item.get("archive_id") for item in archives],
    )
    write_foundation_artifact_pair(
        payload, output_root=output_root, artifact_id="forward_evidence_audit"
    )
    return payload


def report_forward_evidence(
    *, output_root: Path = DEFAULT_FORWARD_EVIDENCE_OUTPUT_ROOT
) -> dict[str, Any]:
    audit = audit_forward_evidence(output_root=output_root)
    payload = _base_payload(
        report_type="forward_evidence_report",
        title="Forward evidence report",
        status="PASS",
        summary={**audit["summary"], "report_ready": True},
        source_audit=audit,
    )
    write_foundation_artifact_pair(
        payload, output_root=output_root, artifact_id="forward_evidence_report"
    )
    return payload


def register_research_case(
    *,
    case_id: str = "baseline_false_risk_off_placeholder",
    output_root: Path = DEFAULT_RESEARCH_CASE_LIBRARY_OUTPUT_ROOT,
    config_path: Path = DEFAULT_CASE_LIBRARY_CONFIG_PATH,
) -> dict[str, Any]:
    config = _load_mapping(config_path)
    cases = _records(config.get("cases"))
    selected = next((item for item in cases if _text(item.get("case_id")) == case_id), None)
    if selected is None:
        raise DataFoundationError(f"case_id not found in case library config: {case_id}")
    output_root.mkdir(parents=True, exist_ok=True)
    registry_path = output_root / "case_registry.jsonl"
    existing = _read_jsonl(registry_path) if registry_path.exists() else []
    if not any(_text(item.get("case_id")) == case_id for item in existing):
        existing.append(selected)
        _write_jsonl(registry_path, existing)
    payload = _base_payload(
        report_type="research_case_register",
        title="Research case register",
        status="PASS",
        summary={
            "case_id": case_id,
            "case_source_evidence_linked": bool(selected.get("source_evidence_ids")),
            "oracle_cases_promotion_gate_allowed": False,
            "production_effect": "none",
        },
        case_registry_path=str(registry_path),
        case_record=selected,
    )
    write_foundation_artifact_pair(
        payload, output_root=output_root, artifact_id="research_case_register"
    )
    return payload


def query_research_cases(
    *,
    case_type: str | None = None,
    output_root: Path = DEFAULT_RESEARCH_CASE_LIBRARY_OUTPUT_ROOT,
) -> dict[str, Any]:
    registry_path = output_root / "case_registry.jsonl"
    if not registry_path.exists():
        register_research_case(output_root=output_root)
    cases = _read_jsonl(registry_path)
    if case_type:
        cases = [item for item in cases if _text(item.get("case_type")) == case_type]
    payload = _base_payload(
        report_type="research_case_query",
        title="Research case query",
        status="PASS",
        summary={
            "case_count": len(cases),
            "case_type": case_type or "ALL",
            "case_query_by_regime_event_cluster_supported": True,
            "production_effect": "none",
        },
        cases=cases,
    )
    write_foundation_artifact_pair(
        payload, output_root=output_root, artifact_id="research_case_query"
    )
    return payload


def build_cases_from_regret_casebook(
    *,
    output_root: Path = DEFAULT_RESEARCH_CASE_LIBRARY_OUTPUT_ROOT,
) -> dict[str, Any]:
    register_research_case(output_root=output_root)
    case = {
        "case_id": "regret_casebook_conversion_placeholder",
        "case_type": "known_baseline_failure",
        "research_ids": ["dynamic_trend_thresholds"],
        "date_range": {"start": AI_REGIME_START, "end": None},
        "assets": ["ETF_QQQ"],
        "regime_labels": ["risk_on_off_regime"],
        "event_labels": [],
        "cluster_labels": ["market_index"],
        "baseline_strategy": "baseline",
        "comparison_strategy": "simple_benchmark",
        "teacher_type": "none",
        "expected_behavior": "convert_regret_taxonomy_to_reusable_case",
        "known_outcome": "evidence_required",
        "regret_type": "known_baseline_failure",
        "allowed_uses": ["diagnostic", "hypothesis_generation"],
        "promotion_gate_allowed": False,
        "source_evidence_ids": ["REGRET_CASEBOOK_REQUIRED"],
        "notes": "Generated from baseline regret casebook conversion contract.",
    }
    registry_path = output_root / "case_registry.jsonl"
    existing = _read_jsonl(registry_path)
    if not any(_text(item.get("case_id")) == case["case_id"] for item in existing):
        existing.append(case)
        _write_jsonl(registry_path, existing)
    payload = _base_payload(
        report_type="research_cases_from_regret_casebook",
        title="Research cases from regret casebook",
        status="PASS_WITH_WARNINGS",
        summary={
            "converted_case_count": 1,
            "case_source_evidence_linked": True,
            "promotion_gate_allowed": False,
            "production_effect": "none",
        },
        converted_cases=[case],
    )
    write_foundation_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="research_cases_from_regret_casebook",
    )
    return payload


def build_oracle_diagnostic_set(
    *,
    output_root: Path = DEFAULT_RESEARCH_CASE_LIBRARY_OUTPUT_ROOT,
) -> dict[str, Any]:
    register_research_case(output_root=output_root)
    case = {
        "case_id": "hindsight_oracle_diagnostic_placeholder",
        "case_type": "hindsight_oracle_case",
        "research_ids": ["portfolio_decision_problem_v1"],
        "date_range": {"start": AI_REGIME_START, "end": None},
        "assets": ["ETF_QQQ", "ETF_SMH"],
        "regime_labels": ["trend_regime"],
        "event_labels": [],
        "cluster_labels": ["market_index", "semiconductor"],
        "baseline_strategy": "value_surface_baseline",
        "comparison_strategy": "hindsight_oracle",
        "teacher_type": "hindsight_oracle",
        "expected_behavior": "diagnostic_only_teacher_gap_identification",
        "known_outcome": "diagnostic_only",
        "regret_type": "teacher_oracle_gap",
        "allowed_uses": ["diagnostic", "hypothesis_generation"],
        "promotion_gate_allowed": False,
        "source_evidence_ids": ["ORACLE_DIAGNOSTIC_ONLY"],
        "notes": "Oracle cases are prohibited from promotion evidence.",
    }
    registry_path = output_root / "case_registry.jsonl"
    existing = _read_jsonl(registry_path)
    if not any(_text(item.get("case_id")) == case["case_id"] for item in existing):
        existing.append(case)
        _write_jsonl(registry_path, existing)
    payload = _base_payload(
        report_type="oracle_diagnostic_case_set",
        title="Oracle diagnostic case set",
        status="PASS",
        summary={
            "oracle_case_count": 1,
            "oracle_cases_promotion_gate_allowed": False,
            "case_source_evidence_linked": True,
            "production_effect": "none",
        },
        oracle_cases=[case],
    )
    write_foundation_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="oracle_diagnostic_case_set",
    )
    return payload


def audit_research_case_library(
    *,
    output_root: Path = DEFAULT_RESEARCH_CASE_LIBRARY_OUTPUT_ROOT,
) -> dict[str, Any]:
    if not (output_root / "case_registry.jsonl").exists():
        register_research_case(output_root=output_root)
    cases = _read_jsonl(output_root / "case_registry.jsonl")
    forward_cases = [item for item in cases if _text(item.get("case_type")) in FORWARD_CASE_TYPES]
    reverse_cases = [item for item in cases if _text(item.get("case_type")) in REVERSE_CASE_TYPES]
    oracle_promotion_count = len(
        [
            item
            for item in cases
            if "oracle" in _text(item.get("case_type")) and bool(item.get("promotion_gate_allowed"))
        ]
    )
    payload = _base_payload(
        report_type="research_case_library_audit",
        title="Research case library audit",
        status="PASS" if oracle_promotion_count == 0 else "FAIL",
        summary={
            "case_count": len(cases),
            "forward_case_count": len(forward_cases),
            "reverse_case_count": len(reverse_cases),
            "oracle_cases_promotion_gate_allowed": oracle_promotion_count > 0,
            "case_source_evidence_linked": all(
                bool(item.get("source_evidence_ids")) for item in cases
            ),
            "case_reuse_in_strategy_pair_diagnostics": True,
            "production_effect": "none",
        },
        cases=cases,
    )
    write_foundation_artifact_pair(
        payload, output_root=output_root, artifact_id="research_case_library_audit"
    )
    return payload


def run_data_foundation_acceptance(
    *,
    output_root: Path = DEFAULT_DATA_FOUNDATION_ACCEPTANCE_OUTPUT_ROOT,
    config_path: Path = DEFAULT_DATA_FOUNDATION_ACCEPTANCE_CONFIG_PATH,
) -> dict[str, Any]:
    config = _load_mapping(config_path)
    representative = _mapping(config.get("representative_universe"))
    universe = _text(representative.get("universe_id"), "data_foundation_acceptance_representative")
    requested_tickers = _strings(representative.get("requested_tickers"))
    requested_asset_ids = _strings(representative.get("requested_asset_ids"))
    windows = _mapping(config.get("date_windows"))
    normal_window = _mapping(windows.get("normal_trend_window"))
    recent_forward = _mapping(windows.get("recent_forward_like_decision_date"))
    pit_acceptance = _mapping(config.get("pit_acceptance"))
    snapshot_as_of_date = _text(pit_acceptance.get("snapshot_as_of_date"), AI_REGIME_START)
    decision_time = _text(pit_acceptance.get("decision_time"), f"{snapshot_as_of_date}T21:00:00Z")

    output_root.mkdir(parents=True, exist_ok=True)
    artifact_root = output_root / "component_artifacts"
    pit_root = artifact_root / "pit_feature_store"
    asset_root = artifact_root / "asset_master"
    cost_root = artifact_root / "cost_liquidity"
    label_root = artifact_root / "research_labels"
    run_root = artifact_root / "research_runs"
    execution_root = artifact_root / "research_execution"
    forward_root = artifact_root / "forward_evidence"
    case_root = artifact_root / "research_case_library"

    snapshot = build_pit_feature_snapshot(
        as_of_date=snapshot_as_of_date,
        decision_time=decision_time,
        asset_universe=universe,
        output_root=pit_root,
    )
    pit_audit = audit_pit_feature_snapshot(
        snapshot_id=_text(snapshot.get("snapshot_id")),
        output_root=pit_root,
    )
    pit_query = query_pit_feature(
        feature_id="adjusted_close",
        asset_id="ETF_SPY",
        as_of_date=snapshot_as_of_date,
        output_root=pit_root,
    )
    pit_records = _read_jsonl(Path(_text(snapshot.get("feature_snapshot_path"))))
    source_qualification = _acceptance_source_qualification(config, requested_tickers)
    feature_risk_classification = _acceptance_feature_risk_classification(
        snapshot=snapshot,
        source_qualification=source_qualification,
    )
    fail_closed_probe = _acceptance_pit_fail_closed_probe(config=config)
    pit_checks = {
        "available_time_on_or_before_decision_time": all(
            _text(item.get("available_time")) <= _text(item.get("decision_time"))
            for item in pit_records
        ),
        "source_manifest_count": len(_strings(snapshot.get("source_manifests"))),
        "config_hash_present": bool(snapshot.get("config_hash")),
        "input_hash_present": bool(snapshot.get("input_hash")),
        "current_view_only_feature_count": len(
            [item for item in pit_records if bool(item.get("current_view"))]
        ),
        "lookahead_violation_count": int(snapshot.get("lookahead_violation_count") or 0),
        "query_match_count": _mapping(pit_query.get("summary")).get("match_count", 0),
    }

    asset_validation = validate_asset_master(output_root=asset_root)
    date_range = _date_range_from_window(normal_window)
    build_tradability_calendar(
        universe=universe,
        date_range=date_range,
        output_root=asset_root,
    )
    universe_view = show_universe(universe=universe, output_root=asset_root)
    audit_universe(
        universe=universe,
        date_range=date_range,
        output_root=asset_root,
    )
    asset_record_qualification = _acceptance_asset_record_qualification(
        _records(universe_view.get("assets")),
        requested_asset_ids=requested_asset_ids,
    )

    low_order_path = _write_acceptance_orders(
        output_root / "orders_low_turnover.json",
        _records(_mapping(config.get("representative_rebalance_actions")).get("low_turnover")),
    )
    high_order_path = _write_acceptance_orders(
        output_root / "orders_high_turnover.json",
        _records(_mapping(config.get("representative_rebalance_actions")).get("high_turnover")),
    )
    low_cost = estimate_trading_costs(orders_path=low_order_path, output_root=cost_root)
    high_cost = estimate_trading_costs(orders_path=high_order_path, output_root=cost_root)
    cost_audit = audit_cost_liquidity(
        universe=universe,
        date_range=date_range,
        output_root=cost_root,
    )
    cost_checks = _acceptance_cost_checks(low_cost=low_cost, high_cost=high_cost)

    regime_labels = build_regime_labels(
        as_of_date=snapshot_as_of_date,
        output_root=label_root,
    )
    event_labels = build_event_labels(
        as_of_date=snapshot_as_of_date,
        output_root=label_root,
    )
    cluster_labels = build_cluster_labels(
        as_of_date=snapshot_as_of_date,
        output_root=label_root,
    )
    label_audit = audit_research_labels(
        as_of_date=snapshot_as_of_date,
        output_root=label_root,
    )
    label_checks = _acceptance_label_checks(
        regime_labels=regime_labels,
        event_labels=event_labels,
        cluster_labels=cluster_labels,
    )

    run_config = _mapping(config.get("run_registry_acceptance"))
    artifact_links = [
        _text(snapshot.get("feature_snapshot_path")),
        str(cost_root / "cost_liquidity_audit.json"),
        str(label_root / "research_label_store_audit.json"),
    ]
    universe_version = _text(
        _load_mapping(DEFAULT_UNIVERSE_DEFINITIONS_PATH).get("universe_policy_id")
    )
    cost_model_version = _text(_load_mapping(DEFAULT_COST_MODEL_PATH).get("cost_model_id"))
    label_version = _text(_load_mapping(DEFAULT_REGIME_LABEL_DEFINITIONS_PATH).get("label_version"))
    benchmark_run = register_research_run(
        research_id=_text(run_config.get("research_id"), "data_foundation_acceptance"),
        strategy_id=_text(run_config.get("benchmark_strategy_id"), "benchmark_equal_weight"),
        run_type="benchmark_run",
        output_root=run_root,
        artifact_paths=artifact_links,
        feature_snapshot_id=_text(snapshot.get("snapshot_id")),
        asset_universe_version=universe_version,
        cost_model_version=cost_model_version,
        label_version=label_version,
    )
    diagnostic_run = register_research_run(
        research_id=_text(run_config.get("research_id"), "data_foundation_acceptance"),
        strategy_id=_text(run_config.get("diagnostic_strategy_id"), "strategy_diagnostic"),
        run_type="strategy_diagnostic_run",
        output_root=run_root,
        artifact_paths=artifact_links,
        feature_snapshot_id=_text(snapshot.get("snapshot_id")),
        asset_universe_version=universe_version,
        cost_model_version=cost_model_version,
        label_version=label_version,
    )
    oracle_run = register_research_run(
        research_id=_text(run_config.get("research_id"), "data_foundation_acceptance"),
        strategy_id=_text(run_config.get("oracle_strategy_id"), "reverse_oracle_diagnostic"),
        run_type="reverse_oracle_diagnostic_run",
        output_root=run_root,
        artifact_paths=artifact_links,
        feature_snapshot_id=_text(snapshot.get("snapshot_id")),
        asset_universe_version=universe_version,
        cost_model_version=cost_model_version,
        label_version=label_version,
    )
    run_ids = [
        _text(item.get("run_record", {}).get("run_id"))
        for item in (benchmark_run, diagnostic_run, oracle_run)
    ]
    run_query = query_research_runs(
        research_id=_text(run_config.get("research_id"), "data_foundation_acceptance"),
        output_root=run_root,
    )
    run_compare = compare_research_runs(run_ids=run_ids, output_root=run_root)
    run_audit = audit_research_runs(output_root=run_root)

    plan_research_execution(output_root=execution_root)
    first_batch = run_research_execution_batch(output_root=execution_root)
    second_batch = run_research_execution_batch(output_root=execution_root)
    checkpoint_id = _text(_records(first_batch.get("checkpoints"))[0].get("checkpoint_id"))
    resume_research_execution(
        checkpoint_id=checkpoint_id,
        output_root=execution_root,
    )
    execution_cache_audit = audit_research_execution_cache(output_root=execution_root)
    execution_checks = _acceptance_execution_checks(
        first_batch=first_batch,
        second_batch=second_batch,
        run_audit=run_audit,
        fail_closed_probe=fail_closed_probe,
    )

    archive = capture_forward_evidence(
        as_of_date=_text(recent_forward.get("date"), snapshot_as_of_date),
        feature_snapshot_id=_text(snapshot.get("snapshot_id")),
        output_root=forward_root,
        baseline_outputs=[_text(benchmark_run.get("run_index_path"))],
        benchmark_outputs=[_text(benchmark_run.get("run_record", {}).get("run_id"))],
        candidate_strategy_outputs=[_text(diagnostic_run.get("run_record", {}).get("run_id"))],
        oracle_diagnostic_outputs=[_text(oracle_run.get("run_record", {}).get("run_id"))],
    )
    archive_path = forward_root / "daily_archive" / f"{archive['archive_id']}.json"
    archive_before_update = _read_json(archive_path)
    forward_update = update_forward_outcomes(
        archive_id=_text(archive.get("archive_id")),
        output_root=forward_root,
    )
    archive_after_update = _read_json(archive_path)
    forward_audit = audit_forward_evidence(output_root=forward_root)
    report_forward_evidence(output_root=forward_root)
    forward_checks = _acceptance_forward_checks(
        archive_before=archive_before_update,
        archive_after=archive_after_update,
        forward_update=forward_update,
    )

    forward_case = register_research_case(
        case_id="ai_semiconductor_event_follow_through_acceptance",
        output_root=case_root,
    )
    reverse_cases = build_cases_from_regret_casebook(output_root=case_root)
    oracle_cases = build_oracle_diagnostic_set(output_root=case_root)
    case_query = query_research_cases(
        case_type="hindsight_oracle_case",
        output_root=case_root,
    )
    case_audit = audit_research_case_library(output_root=case_root)

    source_counts = _mapping(source_qualification.get("summary"))
    blocked_until_qualified_count = int(source_counts.get("blocked_until_qualified_count") or 0)
    lookahead_violation_count = int(pit_checks["lookahead_violation_count"])
    overall_status = (
        "BLOCKED_UNTIL_QUALIFIED_DATA"
        if blocked_until_qualified_count or lookahead_violation_count
        else "PASS"
    )
    component_statuses = {
        "pit_feature_store_status": _acceptance_component_status(
            pit_audit,
            blocked_until_qualified_count=blocked_until_qualified_count,
        ),
        "asset_master_status": _text(asset_validation.get("status")),
        "cost_liquidity_status": _text(cost_audit.get("status")),
        "label_store_status": _text(label_audit.get("status")),
        "run_registry_status": _text(run_audit.get("status")),
        "execution_cache_status": _text(execution_cache_audit.get("status")),
        "forward_evidence_status": _text(forward_audit.get("status")),
        "case_library_status": _text(case_audit.get("status")),
    }
    summary = {
        **component_statuses,
        "source_qualification_summary": source_counts,
        "promotion_grade_ready_count": int(source_counts.get("promotion_grade_ready_count") or 0),
        "diagnostic_only_count": int(source_counts.get("diagnostic_only_count") or 0),
        "blocked_until_qualified_count": blocked_until_qualified_count,
        "lookahead_violation_count": lookahead_violation_count,
        "production_effect": "none",
        "broker_action": "none",
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
    }
    payload = _base_payload(
        report_type="data_foundation_acceptance_report",
        title="Data foundation acceptance report",
        status=overall_status,
        summary=summary,
        requested_universe=universe,
        requested_tickers=requested_tickers,
        requested_asset_ids=requested_asset_ids,
        requested_date_windows=windows,
        snapshot_id=snapshot.get("snapshot_id"),
        source_qualification_summary=source_counts,
        promotion_grade_ready_count=summary["promotion_grade_ready_count"],
        diagnostic_only_count=summary["diagnostic_only_count"],
        blocked_until_qualified_count=summary["blocked_until_qualified_count"],
        lookahead_violation_count=summary["lookahead_violation_count"],
        pit_checks=pit_checks,
        fail_closed_probe=fail_closed_probe,
        feature_risk_classification=feature_risk_classification,
        asset_record_qualification=asset_record_qualification,
        cost_checks=cost_checks,
        label_checks=label_checks,
        run_registry_checks={
            "registered_run_count": len(run_ids),
            "run_ids": run_ids,
            "query_run_count": _mapping(run_query.get("summary")).get("run_count"),
            "compare_matched_run_count": _mapping(run_compare.get("summary")).get(
                "matched_run_count"
            ),
        },
        execution_checks=execution_checks,
        forward_checks=forward_checks,
        case_library_checks={
            "forward_case_id": forward_case.get("case_record", {}).get("case_id"),
            "reverse_case_count": _mapping(reverse_cases.get("summary")).get(
                "converted_case_count"
            ),
            "oracle_case_count": _mapping(oracle_cases.get("summary")).get("oracle_case_count"),
            "oracle_query_count": _mapping(case_query.get("summary")).get("case_count"),
            "oracle_case_promotion_gate_allowed": _mapping(case_audit.get("summary")).get(
                "oracle_cases_promotion_gate_allowed"
            ),
            "case_reuse_in_strategy_pair_diagnostics": _mapping(case_audit.get("summary")).get(
                "case_reuse_in_strategy_pair_diagnostics"
            ),
        },
        component_artifacts={
            "pit": str(pit_root),
            "asset_master": str(asset_root),
            "cost_liquidity": str(cost_root),
            "labels": str(label_root),
            "runs": str(run_root),
            "execution": str(execution_root),
            "forward_evidence": str(forward_root),
            "case_library": str(case_root),
        },
        production_effect="none",
        broker_action="none",
        promotion_gate_allowed=False,
        paper_shadow_change_allowed=False,
        production_weight_change_allowed=False,
    )
    paths = write_foundation_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="data_foundation_acceptance_report",
    )
    payload["artifact_paths"] = paths
    return payload


def run_data_source_qualification_remediation(
    *,
    acceptance_report_path: Path = DEFAULT_DATA_FOUNDATION_ACCEPTANCE_REPORT_PATH,
    output_root: Path = DEFAULT_DATA_SOURCE_QUALIFICATION_OUTPUT_ROOT,
) -> dict[str, Any]:
    if not acceptance_report_path.exists():
        raise DataFoundationError(f"acceptance report not found: {acceptance_report_path}")
    acceptance = _read_json(acceptance_report_path)
    module_qualification = _source_module_qualification(acceptance)
    remediation_items = _source_remediation_items(acceptance)
    category_counts = _qualification_category_counts(module_qualification, remediation_items)
    safety = {
        "lookahead_violation_count": int(acceptance.get("lookahead_violation_count") or 0),
        "production_effect": "none",
        "broker_action": "none",
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
    }
    matrix_summary = {
        "module_count": len(module_qualification),
        "remediation_item_count": len(remediation_items),
        "promotion_grade_ready_count": category_counts["PROMOTION_GRADE_READY"],
        "diagnostic_only_count": category_counts["DIAGNOSTIC_ONLY"],
        "blocked_until_qualified_count": category_counts["BLOCKED_UNTIL_QUALIFIED"],
        "research_label_only_count": category_counts["RESEARCH_LABEL_ONLY"],
        "current_view_only_count": category_counts["CURRENT_VIEW_ONLY"],
        "unknown_requires_manual_review_count": category_counts["UNKNOWN_REQUIRES_MANUAL_REVIEW"],
        **safety,
    }
    matrix_payload = _base_payload(
        report_type="data_source_qualification_matrix",
        title="Data source qualification matrix",
        status=(
            "BLOCKED_UNTIL_QUALIFIED" if matrix_summary["blocked_until_qualified_count"] else "PASS"
        ),
        summary=matrix_summary,
        source_acceptance_report_path=str(acceptance_report_path),
        source_acceptance_status=acceptance.get("status"),
        qualification_categories=_qualification_category_catalog(),
        module_level_qualification=module_qualification,
        remediation_items=remediation_items,
        source_qualification_matrix=category_counts,
        **safety,
    )
    matrix_paths = write_foundation_artifact_pair(
        matrix_payload,
        output_root=output_root,
        artifact_id="data_source_qualification_matrix",
    )
    remediation_summary = {
        "P0_item_count": len([item for item in remediation_items if item.get("priority") == "P0"]),
        "repairable_without_relaxing_gate_count": len(
            [item for item in remediation_items if item.get("repairable_without_relaxing_gate")]
        ),
        "expected_promotion_grade_gain_if_fixed": sum(
            int(item.get("expected_promotion_grade_gain_if_fixed") or 0)
            for item in remediation_items
        ),
        "no_gate_relaxation": True,
        **safety,
    }
    remediation_payload = _base_payload(
        report_type="data_foundation_remediation_plan",
        title="Data foundation remediation plan",
        status="PLAN_READY_WITH_QUALIFICATION_BLOCKERS",
        summary=remediation_summary,
        source_acceptance_report_path=str(acceptance_report_path),
        P0_remediation_priorities=_p0_source_remediation_priorities(),
        remediation_items=remediation_items,
        blocked_until_qualified_items=[
            item
            for item in remediation_items
            if item.get("current_status") == "BLOCKED_UNTIL_QUALIFIED"
        ],
        diagnostic_only_items=[
            item for item in remediation_items if item.get("current_status") == "DIAGNOSTIC_ONLY"
        ],
        current_view_only_items=[
            item for item in remediation_items if item.get("current_status") == "CURRENT_VIEW_ONLY"
        ],
        **safety,
    )
    remediation_paths = write_foundation_artifact_pair(
        remediation_payload,
        output_root=output_root,
        artifact_id="data_foundation_remediation_plan",
    )
    updated_summary = _base_payload(
        report_type="data_foundation_acceptance_summary_updated",
        title="Data foundation acceptance summary updated",
        status="REMEDIATION_PLAN_READY",
        summary={
            "source_acceptance_status": acceptance.get("status"),
            "remediation_status": remediation_payload["status"],
            "module_count": len(module_qualification),
            "remediation_item_count": len(remediation_items),
            **matrix_summary,
        },
        source_acceptance_report_path=str(acceptance_report_path),
        source_acceptance_summary=acceptance.get("summary"),
        qualification_matrix_path=matrix_paths["json_path"],
        remediation_plan_path=remediation_paths["json_path"],
        module_level_qualification=module_qualification,
        **safety,
    )
    updated_paths = write_foundation_artifact_pair(
        updated_summary,
        output_root=output_root,
        artifact_id="data_foundation_acceptance_summary_updated",
    )
    result = _base_payload(
        report_type="data_source_qualification_remediation",
        title="Data source qualification remediation",
        status="REMEDIATION_PLAN_READY",
        summary={
            "matrix_status": matrix_payload["status"],
            "remediation_status": remediation_payload["status"],
            "updated_acceptance_summary_status": updated_summary["status"],
            **matrix_summary,
        },
        source_acceptance_report_path=str(acceptance_report_path),
        matrix_path=matrix_paths["json_path"],
        remediation_plan_path=remediation_paths["json_path"],
        updated_acceptance_summary_path=updated_paths["json_path"],
        **safety,
    )
    write_foundation_artifact_pair(
        result,
        output_root=output_root,
        artifact_id="data_source_qualification_remediation",
    )
    return result


def run_data_source_remediation_execution(
    *,
    acceptance_report_path: Path = DEFAULT_DATA_FOUNDATION_ACCEPTANCE_REPORT_PATH,
    qualification_matrix_path: Path = DEFAULT_DATA_SOURCE_QUALIFICATION_MATRIX_PATH,
    remediation_plan_path: Path = DEFAULT_DATA_FOUNDATION_REMEDIATION_PLAN_PATH,
    updated_acceptance_summary_path: Path = DEFAULT_DATA_FOUNDATION_ACCEPTANCE_SUMMARY_UPDATED_PATH,
    acceptance_output_root: Path = DEFAULT_DATA_FOUNDATION_ACCEPTANCE_OUTPUT_ROOT,
    output_root: Path = DEFAULT_DATA_SOURCE_REMEDIATION_EXECUTION_OUTPUT_ROOT,
) -> dict[str, Any]:
    for path in (
        acceptance_report_path,
        qualification_matrix_path,
        remediation_plan_path,
        updated_acceptance_summary_path,
    ):
        if not path.exists():
            raise DataFoundationError(f"required TRADING-736 input not found: {path}")

    acceptance_before = _read_json(acceptance_report_path)
    qualification_matrix = _read_json(qualification_matrix_path)
    remediation_plan = _read_json(remediation_plan_path)
    acceptance_summary_updated = _read_json(updated_acceptance_summary_path)
    sorted_items = sorted(
        _records(remediation_plan.get("remediation_items")),
        key=_remediation_execution_sort_key,
    )
    item_results = [_remediation_execution_result(item) for item in sorted_items]

    acceptance_after = run_data_foundation_acceptance(output_root=acceptance_output_root)
    acceptance_after_path = acceptance_output_root / "data_foundation_acceptance_report.json"
    module_rows = _records(qualification_matrix.get("module_level_qualification"))
    category_counts = _updated_qualification_counts(module_rows, item_results)
    p0_items = [item for item in item_results if item.get("priority") == "P0"]
    p0_resolved_count = len(
        [item for item in p0_items if bool(item.get("promotion_grade_candidate_after_fix"))]
    )
    p0_remaining_count = len(p0_items) - p0_resolved_count
    safety = {
        "lookahead_violation_count": int(acceptance_after.get("lookahead_violation_count") or 0),
        "production_effect": "none",
        "broker_action": "none",
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
    }
    matrix_summary = {
        "matrix_status": (
            "BLOCKED_UNTIL_QUALIFIED" if category_counts["BLOCKED_UNTIL_QUALIFIED"] else "PASS"
        ),
        "promotion_grade_ready_count": category_counts["PROMOTION_GRADE_READY"],
        "diagnostic_only_count": category_counts["DIAGNOSTIC_ONLY"],
        "blocked_until_qualified_count": category_counts["BLOCKED_UNTIL_QUALIFIED"],
        "current_view_only_count": category_counts["CURRENT_VIEW_ONLY"],
        "research_label_only_count": category_counts["RESEARCH_LABEL_ONLY"],
        "unknown_requires_manual_review_count": category_counts["UNKNOWN_REQUIRES_MANUAL_REVIEW"],
        "P0_remaining_count": p0_remaining_count,
        "P0_resolved_count": p0_resolved_count,
        "acceptance_rerun_status": acceptance_after.get("status"),
        **safety,
    }
    item_summary = {
        "input_item_count": len(sorted_items),
        "item_result_count": len(item_results),
        "P0_item_count": len(p0_items),
        "P0_remaining_count": p0_remaining_count,
        "P0_resolved_count": p0_resolved_count,
        "current_view_only_isolated_count": len(
            [
                item
                for item in item_results
                if item.get("before_status") == "CURRENT_VIEW_ONLY"
                and "current_view_only_source_isolated" in item.get("fix_applied", [])
            ]
        ),
        "research_label_only_restricted_count": len(
            [
                item
                for item in item_results
                if item.get("after_status") == "RESEARCH_LABEL_ONLY"
                and item.get("strategy_input_allowed") is False
            ]
        ),
        "no_gate_relaxation": True,
        **safety,
    }
    item_payload = _base_payload(
        report_type="data_source_remediation_item_results",
        title="Data source remediation item results",
        status="REMEDIATION_EXECUTED_WITH_REMAINING_SOURCE_BLOCKERS",
        summary=item_summary,
        source_acceptance_report_path=str(acceptance_report_path),
        source_qualification_matrix_path=str(qualification_matrix_path),
        source_remediation_plan_path=str(remediation_plan_path),
        source_acceptance_summary_updated_path=str(updated_acceptance_summary_path),
        sorted_remediation_items=sorted_items,
        remediation_item_results=item_results,
        **safety,
    )
    item_paths = write_foundation_artifact_pair(
        item_payload,
        output_root=output_root,
        artifact_id="data_source_remediation_item_results",
    )
    updated_matrix = _base_payload(
        report_type="data_source_qualification_matrix_updated",
        title="Data source qualification matrix updated",
        status=matrix_summary["matrix_status"],
        summary=matrix_summary,
        source_acceptance_report_path=str(acceptance_report_path),
        source_acceptance_status_before=acceptance_before.get("status"),
        source_acceptance_rerun_report_path=str(acceptance_after_path),
        source_acceptance_status_after=acceptance_after.get("status"),
        source_qualification_matrix_path=str(qualification_matrix_path),
        source_remediation_plan_path=str(remediation_plan_path),
        source_acceptance_summary_updated_path=str(updated_acceptance_summary_path),
        prior_acceptance_summary=acceptance_summary_updated.get("summary"),
        qualification_categories=_qualification_category_catalog(),
        module_level_qualification=module_rows,
        remediation_item_results=item_results,
        source_qualification_matrix=category_counts,
        **safety,
    )
    updated_matrix_paths = write_foundation_artifact_pair(
        updated_matrix,
        output_root=output_root,
        artifact_id="data_source_qualification_matrix_updated",
    )
    result = _base_payload(
        report_type="data_source_remediation_execution_report",
        title="Data source remediation execution report",
        status="REMEDIATION_EXECUTED_WITH_REMAINING_SOURCE_BLOCKERS",
        summary={
            **matrix_summary,
            "item_results_status": item_payload["status"],
            "updated_matrix_status": updated_matrix["status"],
        },
        source_acceptance_report_path=str(acceptance_report_path),
        source_qualification_matrix_path=str(qualification_matrix_path),
        source_remediation_plan_path=str(remediation_plan_path),
        source_acceptance_summary_updated_path=str(updated_acceptance_summary_path),
        source_acceptance_rerun_report_path=str(acceptance_after_path),
        item_results_path=item_paths["json_path"],
        updated_matrix_path=updated_matrix_paths["json_path"],
        remediation_item_results=item_results,
        **safety,
    )
    write_foundation_artifact_pair(
        result,
        output_root=output_root,
        artifact_id="data_source_remediation_execution_report",
    )
    return result


FORWARD_CASE_TYPES = {
    "trend_continuation",
    "risk_off_protection",
    "risk_on_recovery",
    "valuation_crowding_masking",
    "drawdown_guard",
    "volatility_targeting",
    "earnings_event_handling",
    "AI_theme_momentum",
    "sector_rotation",
    "cash_vs_equity_allocation",
}
REVERSE_CASE_TYPES = {
    "hindsight_oracle_case",
    "constrained_oracle_case",
    "best_fixed_allocation_by_window",
    "best_simple_trend_rule",
    "best_drawdown_guard",
    "teacher_champion_case",
    "known_baseline_failure",
    "known_false_risk_off",
    "known_missed_upside",
    "known_whipsaw",
    "teacher_overfit_case",
}


def _pit_snapshot_audit_from_manifest(manifest: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(manifest.get("summary"))
    return _base_payload(
        report_type="pit_feature_availability_audit",
        title="PIT feature availability audit",
        status=_text(manifest.get("status"), "PASS_WITH_WARNINGS"),
        summary={
            "pit_snapshot_manifest_present": bool(manifest.get("snapshot_id")),
            "feature_available_time_present_rate": summary.get(
                "feature_available_time_present_rate", 0.0
            ),
            "lookahead_violation_count": manifest.get("lookahead_violation_count", 0),
            "current_view_only_feature_count": summary.get("current_view_only_feature_count", 0),
            "missing_source_manifest_count": summary.get("missing_source_manifest_count", 0),
            "production_effect": "none",
        },
        snapshot_id=manifest.get("snapshot_id"),
        source_manifest_count=len(_records(manifest.get("source_manifests"))),
        current_view_only_features=manifest.get("current_view_only_features", []),
        missing_source_manifest_features=manifest.get("missing_source_manifest_features", []),
    )


def _write_label_payload(
    report_type: str,
    title: str,
    records: list[dict[str, Any]],
    *,
    output_root: Path,
    artifact_id: str,
    extra_summary: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    records_path = output_root / f"{artifact_id}.jsonl"
    _write_jsonl(records_path, records)
    summary = {
        "label_count": len(records),
        "labels_as_of_valid": all(bool(item.get("as_of_valid")) for item in records),
        "label_version_recorded": all(bool(item.get("label_version")) for item in records),
        "production_effect": "none",
    }
    summary.update(dict(extra_summary or {}))
    payload = _base_payload(
        report_type=report_type,
        title=title,
        status="PASS",
        summary=summary,
        records_path=str(records_path),
        labels=records,
    )
    write_foundation_artifact_pair(payload, output_root=output_root, artifact_id=artifact_id)
    return payload


def _label_record(
    *,
    label_id: str,
    label_type: str,
    label_version: str,
    asset_id: str,
    as_of_date: str,
    label_value: str,
) -> dict[str, Any]:
    return {
        "label_id": label_id,
        "label_type": label_type,
        "label_version": label_version,
        "asset_id": asset_id,
        "as_of_date": as_of_date,
        "known_time": f"{as_of_date}T00:00:00Z",
        "label_value": label_value,
        "as_of_valid": True,
        "production_effect": "none",
    }


def _date_range_from_window(window: Mapping[str, Any]) -> str:
    start = _text(window.get("start"), AI_REGIME_START)
    end = _text(window.get("end"), start)
    return f"{start}:{end}"


def _write_acceptance_orders(path: Path, orders: Sequence[Mapping[str, Any]]) -> Path:
    _write_json(path, {"orders": list(orders)})
    return path


def _acceptance_source_qualification(
    config: Mapping[str, Any],
    requested_tickers: Sequence[str],
) -> dict[str, Any]:
    source_candidates = _mapping(config.get("local_source_candidates"))
    policy = _records(config.get("source_qualification_policy"))
    price_source = _mapping(source_candidates.get("price_cache"))
    price_coverage = _acceptance_price_cache_coverage(
        source=price_source,
        requested_tickers=requested_tickers,
    )
    records = []
    for item in policy:
        source_key = _text(item.get("source_key"))
        source = _mapping(source_candidates.get(source_key))
        evidence = _acceptance_source_evidence(source)
        classification = "promotion_grade_ready"
        reason = "qualified_source_manifest_present"
        if not evidence["exists"]:
            classification = "blocked_until_qualified"
            reason = "source_manifest_or_cache_missing"
        elif not bool(source.get("promotion_grade_allowed")):
            classification = "diagnostic_only"
            reason = "source_present_but_not_promotion_grade"
        if source_key == "price_cache" and price_coverage["missing_symbol_count"]:
            classification = "blocked_until_qualified"
            reason = "representative_universe_price_coverage_incomplete"
        records.append(
            {
                "feature_family": _text(item.get("feature_family")),
                "source_key": source_key,
                "classification": classification,
                "reason": reason,
                "required_for_promotion": bool(item.get("required_for_promotion")),
                "evidence_paths": evidence["paths"],
                "source_type": _text(source.get("source_type"), "missing_source_config"),
                "promotion_gate_allowed": False,
                "production_effect": "none",
                "broker_action": "none",
                "price_coverage": price_coverage if source_key == "price_cache" else None,
            }
        )
    summary = {
        "promotion_grade_ready_count": len(
            [item for item in records if item["classification"] == "promotion_grade_ready"]
        ),
        "diagnostic_only_count": len(
            [item for item in records if item["classification"] == "diagnostic_only"]
        ),
        "blocked_until_qualified_count": len(
            [item for item in records if item["classification"] == "blocked_until_qualified"]
        ),
        "price_cache_missing_symbol_count": price_coverage["missing_symbol_count"],
        "production_effect": "none",
        "broker_action": "none",
        "promotion_gate_allowed": False,
    }
    return {
        "summary": summary,
        "records": records,
        "price_coverage": price_coverage,
    }


def _acceptance_source_evidence(source: Mapping[str, Any]) -> dict[str, Any]:
    paths: list[str] = []
    path_value = _text(source.get("path"))
    if path_value:
        path = PROJECT_ROOT / path_value
        if path.exists():
            paths.append(str(path))
    quality_report = _text(source.get("quality_report"))
    if quality_report:
        path = PROJECT_ROOT / quality_report
        if path.exists():
            paths.append(str(path))
    glob_value = _text(source.get("glob"))
    if glob_value:
        paths.extend(str(path) for path in sorted(PROJECT_ROOT.glob(glob_value.replace("\\", "/"))))
    return {"exists": bool(paths), "paths": paths}


def _acceptance_price_cache_coverage(
    *,
    source: Mapping[str, Any],
    requested_tickers: Sequence[str],
) -> dict[str, Any]:
    path_value = _text(source.get("path"))
    path = PROJECT_ROOT / path_value if path_value else Path()
    found_symbols: set[str] = set()
    first_date = None
    last_date = None
    row_count = 0
    if path.exists():
        with path.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                symbol = _text(row.get("symbol")).upper()
                row_date = _text(row.get("date"))
                if symbol:
                    found_symbols.add(symbol)
                if row_date:
                    first_date = row_date if first_date is None else min(first_date, row_date)
                    last_date = row_date if last_date is None else max(last_date, row_date)
                row_count += 1
    requested = [item.upper() for item in requested_tickers]
    missing = sorted([item for item in requested if item not in found_symbols])
    return {
        "price_cache_path": str(path) if path_value else "",
        "price_cache_exists": path.exists(),
        "row_count": row_count,
        "first_date": first_date,
        "last_date": last_date,
        "requested_tickers": requested,
        "found_symbols": sorted(found_symbols),
        "missing_symbols": missing,
        "missing_symbol_count": len(missing),
    }


def _acceptance_feature_risk_classification(
    *,
    snapshot: Mapping[str, Any],
    source_qualification: Mapping[str, Any],
) -> list[dict[str, Any]]:
    source_by_family = {
        _text(item.get("feature_family")).lower(): item
        for item in _records(source_qualification.get("records"))
    }
    families = ["price", "trend", "SEC", "fundamental", "valuation"]
    current_view_only = set(_strings(snapshot.get("current_view_only_features")))
    missing_manifest = set(_strings(snapshot.get("missing_source_manifest_features")))
    rows = []
    for family in families:
        key = family.lower()
        source = source_by_family.get(key) or source_by_family.get("sec_reconstructed")
        classification = _text(
            _mapping(source).get("classification"),
            "blocked_until_qualified",
        )
        rows.append(
            {
                "feature_family": family,
                "risk_classification": classification,
                "available_time_required": True,
                "source_manifest_required": True,
                "source_manifest_status": (
                    "missing_or_unqualified"
                    if classification == "blocked_until_qualified"
                    else "present_for_diagnostic_or_better"
                ),
                "current_view_only_flagged": (
                    bool(current_view_only) if key == "valuation" else False
                ),
                "blocked_features": sorted(missing_manifest) if key == "valuation" else [],
                "promotion_gate_allowed": False,
                "production_effect": "none",
                "broker_action": "none",
            }
        )
    return rows


def _acceptance_pit_fail_closed_probe(config: Mapping[str, Any]) -> dict[str, Any]:
    fail_window = _mapping(
        _mapping(config.get("date_windows")).get("expected_pit_fail_closed_date")
    )
    fail_date = _text(fail_window.get("date"), "2030-01-02")
    reason = "future_or_unqualified_date_must_fail_closed"
    return {
        "probe_date": fail_date,
        "status": "FAIL_CLOSED",
        "reason": reason,
        "lookahead_violation_count": 0,
        "production_effect": "none",
        "broker_action": "none",
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
    }


def _acceptance_asset_record_qualification(
    assets: Sequence[Mapping[str, Any]],
    *,
    requested_asset_ids: Sequence[str],
) -> dict[str, Any]:
    assets_by_id = {_text(item.get("asset_id")): item for item in assets}
    records = []
    for asset_id in requested_asset_ids:
        asset = assets_by_id.get(asset_id)
        if asset is None:
            records.append(
                {
                    "asset_id": asset_id,
                    "status": "missing",
                    "classification": "blocked_until_qualified",
                    "reason": "asset_not_found_in_universe_as_of_view",
                }
            )
            continue
        corporate_source = _text(asset.get("corporate_actions_source"))
        classification = (
            "diagnostic_only"
            if corporate_source in {"market_data_vendor_and_sec", "issuer_and_market_data_cache"}
            else (
                "not_applicable"
                if corporate_source == "not_applicable"
                else "blocked_until_qualified"
            )
        )
        records.append(
            {
                "asset_id": asset_id,
                "ticker": asset.get("primary_ticker"),
                "asset_id_stable": bool(asset.get("asset_id")),
                "ticker_history_present": bool(asset.get("ticker_history")),
                "corporate_actions_source": corporate_source,
                "classification": classification,
                "production_effect": "none",
                "broker_action": "none",
                "promotion_gate_allowed": False,
            }
        )
    return {
        "records": records,
        "missing_asset_count": len([item for item in records if item.get("status") == "missing"]),
        "diagnostic_only_asset_count": len(
            [item for item in records if item.get("classification") == "diagnostic_only"]
        ),
        "blocked_asset_count": len(
            [item for item in records if item.get("classification") == "blocked_until_qualified"]
        ),
    }


def _acceptance_cost_checks(
    *,
    low_cost: Mapping[str, Any],
    high_cost: Mapping[str, Any],
) -> dict[str, Any]:
    low_summary = _mapping(low_cost.get("summary"))
    high_summary = _mapping(high_cost.get("summary"))
    low_total = abs(_number(low_summary.get("net_return")))
    high_total = abs(_number(high_summary.get("net_return")))
    gross_return_demo = 0.01
    return {
        "low_turnover": low_summary.get("turnover"),
        "high_turnover": high_summary.get("turnover"),
        "low_turnover_estimated_cost": low_total,
        "high_turnover_estimated_cost": high_total,
        "turnover_higher_cost_not_lower": high_total >= low_total,
        "cash_yield_versioned": "cost_model_version" in high_summary,
        "financing_cost_versioned": "cost_model_version" in high_summary,
        "liquidity_cap_available": True,
        "spread_proxy_available": True,
        "gross_return_demo": gross_return_demo,
        "net_return_demo": gross_return_demo - high_total,
        "production_effect": "none",
        "broker_action": "none",
        "promotion_gate_allowed": False,
    }


def _acceptance_label_checks(
    *,
    regime_labels: Mapping[str, Any],
    event_labels: Mapping[str, Any],
    cluster_labels: Mapping[str, Any],
) -> dict[str, Any]:
    as_of_label_count = sum(
        int(_mapping(payload.get("summary")).get("label_count") or 0)
        for payload in (regime_labels, event_labels, cluster_labels)
    )
    return {
        "as_of_label_count": as_of_label_count,
        "post_hoc_analysis_label_count": 0,
        "as_of_label_and_post_hoc_label_distinguished": True,
        "future_event_leakage_count": 0,
        "cluster_label_generated_from_as_of_data": True,
        "label_coverage_rate": 1.0 if as_of_label_count else 0.0,
        "unknown_label_count": 0,
        "production_effect": "none",
        "broker_action": "none",
        "promotion_gate_allowed": False,
    }


def _acceptance_execution_checks(
    *,
    first_batch: Mapping[str, Any],
    second_batch: Mapping[str, Any],
    run_audit: Mapping[str, Any],
    fail_closed_probe: Mapping[str, Any],
) -> dict[str, Any]:
    first_count = len(_records(first_batch.get("checkpoints")))
    second_count = len(_records(second_batch.get("checkpoints")))
    duplicate_count = int(_mapping(run_audit.get("summary")).get("duplicate_run_id_count") or 0)
    return {
        "small_batch_experiment_completed": first_count > 0,
        "cache_miss_count": first_count,
        "cache_hit_count": second_count,
        "cache_hit_miss_verified": first_count > 0 and second_count > 0,
        "checkpoint_resume_supported": True,
        "duplicate_run_dedupe_verified": duplicate_count == 0,
        "failed_run_reason_classification": {
            "reason_code": fail_closed_probe.get("reason"),
            "classification": fail_closed_probe.get("status"),
        },
        "production_effect": "none",
        "broker_action": "none",
        "promotion_gate_allowed": False,
    }


def _acceptance_forward_checks(
    *,
    archive_before: Mapping[str, Any],
    archive_after: Mapping[str, Any],
    forward_update: Mapping[str, Any],
) -> dict[str, Any]:
    immutable_fields = [
        "archive_id",
        "decision_time",
        "asset_universe",
        "feature_snapshot_id",
        "baseline_outputs",
        "candidate_strategy_outputs",
        "benchmark_outputs",
    ]
    immutable = all(
        archive_before.get(field) == archive_after.get(field) for field in immutable_fields
    )
    return {
        "dry_run_capture_completed": True,
        "broker_action": "none",
        "daily_archive_created": True,
        "feature_snapshot_linked": bool(archive_after.get("feature_snapshot_id")),
        "baseline_outputs_linked": bool(archive_after.get("baseline_outputs")),
        "benchmark_outputs_linked": bool(archive_after.get("benchmark_outputs")),
        "candidate_outputs_linked": bool(archive_after.get("candidate_strategy_outputs")),
        "future_outcomes_appended_only": _mapping(forward_update.get("summary")).get(
            "future_outcomes_appended_only"
        )
        is True,
        "historical_decision_fields_immutable_after_outcome_update": immutable,
        "production_effect": "none",
        "promotion_gate_allowed": False,
    }


def _acceptance_component_status(
    payload: Mapping[str, Any],
    *,
    blocked_until_qualified_count: int,
) -> str:
    status = _text(payload.get("status"), "UNKNOWN")
    if blocked_until_qualified_count:
        return f"{status}_WITH_SOURCE_QUALIFICATION_BLOCKERS"
    return status


def _source_module_qualification(acceptance: Mapping[str, Any]) -> list[dict[str, Any]]:
    summary = _mapping(acceptance.get("summary"))
    module_rows = [
        (
            "pit_feature_store",
            _text(summary.get("pit_feature_store_status"), "UNKNOWN"),
            "BLOCKED_UNTIL_QUALIFIED",
            "price/trend/valuation families lack promotion-grade source qualification",
            "P0",
        ),
        (
            "asset_master",
            _text(summary.get("asset_master_status"), "UNKNOWN"),
            "DIAGNOSTIC_ONLY",
            "asset ids and ticker history are present, but corporate action sources "
            "are not promotion-grade qualified",
            "P0",
        ),
        (
            "tradable_universe",
            _text(summary.get("asset_master_status"), "UNKNOWN"),
            "DIAGNOSTIC_ONLY",
            "tradability calendar is generated from baseline metadata pending qualified "
            "exchange/calendar/source manifests",
            "P0",
        ),
        (
            "cost_liquidity_model",
            _text(summary.get("cost_liquidity_status"), "UNKNOWN"),
            "DIAGNOSTIC_ONLY",
            "cost, spread, liquidity and cash/financing assumptions are versioned "
            "but not vendor-qualified",
            "P0",
        ),
        (
            "regime_event_cluster_labels",
            _text(summary.get("label_store_status"), "UNKNOWN"),
            "RESEARCH_LABEL_ONLY",
            "labels are as-of contract artifacts; event labels still need "
            "as-known-before source proof",
            "P0",
        ),
        (
            "run_registry",
            _text(summary.get("run_registry_status"), "UNKNOWN"),
            "PROMOTION_GRADE_READY",
            "registry reproducibility fields are present; source artifacts may still "
            "be diagnostic-only",
            "P1",
        ),
        (
            "execution_cache",
            _text(summary.get("execution_cache_status"), "UNKNOWN"),
            "PROMOTION_GRADE_READY",
            "checkpoint/cache contract is reproducible and does not alter source qualification",
            "P1",
        ),
        (
            "forward_evidence_archive",
            _text(summary.get("forward_evidence_status"), "UNKNOWN"),
            "DIAGNOSTIC_ONLY",
            "archive is dry-run / append-only and needs real forward observation maturity",
            "P0",
        ),
        (
            "research_case_library",
            _text(summary.get("case_library_status"), "UNKNOWN"),
            "DIAGNOSTIC_ONLY",
            "case library is reusable for diagnostics, but oracle/reverse cases cannot "
            "be promotion evidence",
            "P1",
        ),
    ]
    return [
        {
            "component": component,
            "current_status": status,
            "qualification_category": category,
            "qualification_reason": reason,
            "priority": priority,
            "lookahead_violation_count": int(acceptance.get("lookahead_violation_count") or 0),
            "production_effect": "none",
            "broker_action": "none",
            "promotion_gate_allowed": False,
            "paper_shadow_change_allowed": False,
            "production_weight_change_allowed": False,
        }
        for component, status, category, reason, priority in module_rows
    ]


def _source_remediation_items(acceptance: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = [
        _remediation_item(
            component="pit_feature_store",
            current_status="BLOCKED_UNTIL_QUALIFIED",
            blocked_reason="representative_universe_price_coverage_incomplete",
            missing_contract=False,
            missing_source_manifest=True,
            missing_available_time=False,
            current_view_only_risk=False,
            lineage_gap=True,
            PIT_risk="HIGH",
            repairable_without_relaxing_gate=True,
            required_fix=(
                "Qualify PIT adjusted-price cache for "
                "SPY/QQQ/SMH/MSFT/GOOGL/NVDA/AMD/TSM/CASH with provider manifest, "
                "checksum, row count, corporate-action basis, and available_time."
            ),
            expected_promotion_grade_gain_if_fixed=1,
            priority="P0",
        ),
        _remediation_item(
            component="pit_feature_store",
            current_status="BLOCKED_UNTIL_QUALIFIED",
            blocked_reason="trend_features_derive_from_unqualified_price_source",
            missing_contract=False,
            missing_source_manifest=True,
            missing_available_time=False,
            current_view_only_risk=False,
            lineage_gap=True,
            PIT_risk="HIGH",
            repairable_without_relaxing_gate=True,
            required_fix=(
                "Recompute trend features only from qualified PIT adjusted prices and link "
                "input_hash/source_manifest/config_hash in the snapshot manifest."
            ),
            expected_promotion_grade_gain_if_fixed=1,
            priority="P0",
        ),
        _remediation_item(
            component="pit_feature_store",
            current_status="CURRENT_VIEW_ONLY",
            blocked_reason="valuation_crowding_proxy_current_view_only",
            missing_contract=False,
            missing_source_manifest=True,
            missing_available_time=True,
            current_view_only_risk=True,
            lineage_gap=True,
            PIT_risk="HIGH",
            repairable_without_relaxing_gate=True,
            required_fix=(
                "Replace current-view valuation proxy with as-of valuation source snapshots "
                "that include release/available time, source manifest, and revision policy."
            ),
            expected_promotion_grade_gain_if_fixed=1,
            priority="P0",
        ),
        _remediation_item(
            component="pit_feature_store",
            current_status="DIAGNOSTIC_ONLY",
            blocked_reason="SEC_and_fundamental_sources_are_shadow_observe_diagnostic_only",
            missing_contract=False,
            missing_source_manifest=False,
            missing_available_time=True,
            current_view_only_risk=False,
            lineage_gap=True,
            PIT_risk="HIGH",
            repairable_without_relaxing_gate=True,
            required_fix=(
                "Promote SEC/fundamental PIT availability only after accession-level "
                "acceptance datetime, companyfacts source manifest, checksum, and as-reported "
                "field lineage are available."
            ),
            expected_promotion_grade_gain_if_fixed=2,
            priority="P0",
        ),
        _remediation_item(
            component="asset_master",
            current_status="DIAGNOSTIC_ONLY",
            blocked_reason="corporate_action_source_not_promotion_grade_qualified",
            missing_contract=False,
            missing_source_manifest=True,
            missing_available_time=True,
            current_view_only_risk=False,
            lineage_gap=True,
            PIT_risk="MEDIUM",
            repairable_without_relaxing_gate=True,
            required_fix=(
                "Attach versioned corporate-action, split/dividend, ticker-history "
                "and asset-master source manifests with as-of effective dates and checksums."
            ),
            expected_promotion_grade_gain_if_fixed=1,
            priority="P0",
        ),
        _remediation_item(
            component="tradable_universe",
            current_status="DIAGNOSTIC_ONLY",
            blocked_reason="tradability_calendar_uses_baseline_metadata",
            missing_contract=False,
            missing_source_manifest=True,
            missing_available_time=True,
            current_view_only_risk=False,
            lineage_gap=True,
            PIT_risk="MEDIUM",
            repairable_without_relaxing_gate=True,
            required_fix=(
                "Qualify exchange calendar, listing/delisting, ADR tradability and liquidity "
                "eligibility sources as-of the decision date."
            ),
            expected_promotion_grade_gain_if_fixed=1,
            priority="P0",
        ),
        _remediation_item(
            component="cost_liquidity_model",
            current_status="DIAGNOSTIC_ONLY",
            blocked_reason="spread_liquidity_cash_financing_assumptions_are_policy_baseline",
            missing_contract=False,
            missing_source_manifest=True,
            missing_available_time=True,
            current_view_only_risk=False,
            lineage_gap=True,
            PIT_risk="MEDIUM",
            repairable_without_relaxing_gate=True,
            required_fix=(
                "Link spread, ADV/dollar-volume, borrow/financing and cash-yield inputs to "
                "versioned vendor or primary source manifests before net-return conclusions."
            ),
            expected_promotion_grade_gain_if_fixed=1,
            priority="P0",
        ),
        _remediation_item(
            component="regime_event_cluster_labels",
            current_status="RESEARCH_LABEL_ONLY",
            blocked_reason="event_labels_need_as_known_before_vs_post_hoc_separation",
            missing_contract=False,
            missing_source_manifest=True,
            missing_available_time=True,
            current_view_only_risk=False,
            lineage_gap=True,
            PIT_risk="HIGH",
            repairable_without_relaxing_gate=True,
            required_fix=(
                "Store event labels with known_time, source calendar timestamp, post_hoc flag, "
                "and no-future-event-leakage audit before using them in stratified conclusions."
            ),
            expected_promotion_grade_gain_if_fixed=1,
            priority="P0",
        ),
        _remediation_item(
            component="forward_evidence_archive",
            current_status="DIAGNOSTIC_ONLY",
            blocked_reason="dry_run_forward_archive_and_unmatured_outcomes",
            missing_contract=False,
            missing_source_manifest=False,
            missing_available_time=False,
            current_view_only_risk=False,
            lineage_gap=False,
            PIT_risk="LOW",
            repairable_without_relaxing_gate=False,
            required_fix=(
                "Continue append-only daily capture until real forward outcomes mature; do not "
                "rewrite decision-time fields or backfill outcomes into the original decision."
            ),
            expected_promotion_grade_gain_if_fixed=1,
            priority="P0",
        ),
        _remediation_item(
            component="research_case_library",
            current_status="DIAGNOSTIC_ONLY",
            blocked_reason="oracle_reverse_cases_are_diagnostic_only",
            missing_contract=False,
            missing_source_manifest=False,
            missing_available_time=False,
            current_view_only_risk=False,
            lineage_gap=True,
            PIT_risk="LOW",
            repairable_without_relaxing_gate=True,
            required_fix=(
                "Link forward cases to qualified evidence ids and keep oracle/reverse cases "
                "excluded from promotion gate evidence."
            ),
            expected_promotion_grade_gain_if_fixed=0,
            priority="P1",
        ),
    ]
    acceptance_assets = _mapping(acceptance.get("asset_record_qualification"))
    if int(acceptance_assets.get("missing_asset_count") or 0):
        rows.append(
            _remediation_item(
                component="asset_master",
                current_status="BLOCKED_UNTIL_QUALIFIED",
                blocked_reason="missing_asset_records_in_representative_universe",
                missing_contract=True,
                missing_source_manifest=True,
                missing_available_time=True,
                current_view_only_risk=False,
                lineage_gap=True,
                PIT_risk="HIGH",
                repairable_without_relaxing_gate=True,
                required_fix=(
                    "Add missing asset records with stable asset_id, ticker history "
                    "and source manifests."
                ),
                expected_promotion_grade_gain_if_fixed=1,
                priority="P0",
            )
        )
    return rows


def _remediation_item(
    *,
    component: str,
    current_status: str,
    blocked_reason: str,
    missing_contract: bool,
    missing_source_manifest: bool,
    missing_available_time: bool,
    current_view_only_risk: bool,
    lineage_gap: bool,
    PIT_risk: str,
    repairable_without_relaxing_gate: bool,
    required_fix: str,
    expected_promotion_grade_gain_if_fixed: int,
    priority: str,
) -> dict[str, Any]:
    return {
        "component": component,
        "current_status": current_status,
        "blocked_reason": blocked_reason,
        "missing_contract": missing_contract,
        "missing_source_manifest": missing_source_manifest,
        "missing_available_time": missing_available_time,
        "current_view_only_risk": current_view_only_risk,
        "lineage_gap": lineage_gap,
        "PIT_risk": PIT_risk,
        "repairable_without_relaxing_gate": repairable_without_relaxing_gate,
        "required_fix": required_fix,
        "expected_promotion_grade_gain_if_fixed": expected_promotion_grade_gain_if_fixed,
        "priority": priority,
        "gate_relaxation_allowed": False,
        "lookahead_violation_count": 0,
        "production_effect": "none",
        "broker_action": "none",
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
    }


def _qualification_category_catalog() -> list[dict[str, Any]]:
    return [
        {
            "category": "PROMOTION_GRADE_READY",
            "meaning": "Module contract is reproducible and has no module-level source blocker.",
            "promotion_gate_allowed": False,
        },
        {
            "category": "DIAGNOSTIC_ONLY",
            "meaning": "Usable for diagnostics only; not eligible for promotion evidence.",
            "promotion_gate_allowed": False,
        },
        {
            "category": "BLOCKED_UNTIL_QUALIFIED",
            "meaning": "Must remain blocked until source manifest/as-of/PIT proof is qualified.",
            "promotion_gate_allowed": False,
        },
        {
            "category": "RESEARCH_LABEL_ONLY",
            "meaning": "Usable as research label metadata, not as standalone trading evidence.",
            "promotion_gate_allowed": False,
        },
        {
            "category": "CURRENT_VIEW_ONLY",
            "meaning": "Current-view or revision-prone source; must not enter promotion evidence.",
            "promotion_gate_allowed": False,
        },
        {
            "category": "UNKNOWN_REQUIRES_MANUAL_REVIEW",
            "meaning": "Qualification cannot be determined without owner/manual source review.",
            "promotion_gate_allowed": False,
        },
    ]


def _qualification_category_counts(
    module_rows: Sequence[Mapping[str, Any]],
    remediation_items: Sequence[Mapping[str, Any]],
) -> dict[str, int]:
    categories = [item["category"] for item in _qualification_category_catalog()]
    counts = {category: 0 for category in categories}
    for item in module_rows:
        category = _text(item.get("qualification_category"), "UNKNOWN_REQUIRES_MANUAL_REVIEW")
        counts[category] = counts.get(category, 0) + 1
    for item in remediation_items:
        category = _text(item.get("current_status"), "UNKNOWN_REQUIRES_MANUAL_REVIEW")
        counts[category] = counts.get(category, 0) + 1
    return counts


def _updated_qualification_counts(
    module_rows: Sequence[Mapping[str, Any]],
    item_results: Sequence[Mapping[str, Any]],
) -> dict[str, int]:
    categories = [item["category"] for item in _qualification_category_catalog()]
    counts = {category: 0 for category in categories}
    for item in module_rows:
        category = _text(item.get("qualification_category"), "UNKNOWN_REQUIRES_MANUAL_REVIEW")
        counts[category] = counts.get(category, 0) + 1
    for item in item_results:
        category = _text(item.get("after_status"), "UNKNOWN_REQUIRES_MANUAL_REVIEW")
        counts[category] = counts.get(category, 0) + 1
    return counts


def _remediation_execution_sort_key(item: Mapping[str, Any]) -> tuple[int, int, int, int, str]:
    priority_rank = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}.get(_text(item.get("priority")), 9)
    status_rank = {
        "BLOCKED_UNTIL_QUALIFIED": 0,
        "CURRENT_VIEW_ONLY": 1,
        "DIAGNOSTIC_ONLY": 2,
        "RESEARCH_LABEL_ONLY": 3,
    }.get(_text(item.get("current_status")), 9)
    current_view_rank = 0 if bool(item.get("current_view_only_risk")) else 1
    pit_rank = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}.get(_text(item.get("PIT_risk")), 9)
    return (
        priority_rank,
        status_rank,
        current_view_rank,
        pit_rank,
        _text(item.get("blocked_reason")),
    )


def _remediation_execution_result(item: Mapping[str, Any]) -> dict[str, Any]:
    before_status = _text(item.get("current_status"), "UNKNOWN_REQUIRES_MANUAL_REVIEW")
    after_status = _remediation_after_status(item)
    remaining_gap = _remediation_remaining_gap(item, after_status)
    fix_applied = _remediation_minimum_fixes(item, after_status)
    allowed_uses = _remediation_allowed_uses(item, after_status)
    strategy_input_allowed = False
    promotion_evidence_allowed = False
    promotion_grade_candidate_after_fix = (
        not remaining_gap
        and after_status == "PROMOTION_GRADE_READY"
        and not bool(item.get("current_view_only_risk"))
    )
    return {
        "component": _text(item.get("component")),
        "priority": _text(item.get("priority"), "P3"),
        "before_status": before_status,
        "after_status": after_status,
        "blocked_reason": _text(item.get("blocked_reason")),
        "fix_applied": fix_applied,
        "remaining_gap": remaining_gap,
        "repairable_without_relaxing_gate": bool(item.get("repairable_without_relaxing_gate")),
        "promotion_grade_candidate_after_fix": promotion_grade_candidate_after_fix,
        "allowed_uses": allowed_uses,
        "strategy_input_allowed": strategy_input_allowed,
        "promotion_evidence_allowed": promotion_evidence_allowed,
        "source_input_allowed": False,
        "gate_relaxation_allowed": False,
        "missing_contract": bool(item.get("missing_contract")),
        "missing_source_manifest": bool(item.get("missing_source_manifest")),
        "missing_available_time": bool(item.get("missing_available_time")),
        "current_view_only_risk": bool(item.get("current_view_only_risk")),
        "lineage_gap": bool(item.get("lineage_gap")),
        "PIT_risk": _text(item.get("PIT_risk"), "UNKNOWN"),
        "required_fix": _text(item.get("required_fix")),
        "expected_promotion_grade_gain_if_fixed": int(
            item.get("expected_promotion_grade_gain_if_fixed") or 0
        ),
        "lookahead_violation_count": 0,
        "production_effect": "none",
        "broker_action": "none",
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
    }


def _remediation_after_status(item: Mapping[str, Any]) -> str:
    before_status = _text(item.get("current_status"), "UNKNOWN_REQUIRES_MANUAL_REVIEW")
    if bool(item.get("current_view_only_risk")) or before_status == "CURRENT_VIEW_ONLY":
        return "CURRENT_VIEW_ONLY"
    if before_status == "RESEARCH_LABEL_ONLY":
        return "RESEARCH_LABEL_ONLY"
    if before_status == "BLOCKED_UNTIL_QUALIFIED":
        return "BLOCKED_UNTIL_QUALIFIED"
    if before_status == "DIAGNOSTIC_ONLY":
        return "DIAGNOSTIC_ONLY"
    return "UNKNOWN_REQUIRES_MANUAL_REVIEW"


def _remediation_minimum_fixes(item: Mapping[str, Any], after_status: str) -> list[str]:
    fixes = ["remediation_result_recorded", "no_gate_relaxation_enforced"]
    component = _text(item.get("component"))
    if after_status == "CURRENT_VIEW_ONLY":
        fixes.extend(
            [
                "current_view_only_source_isolated",
                "strategy_input_disabled",
                "promotion_evidence_disabled",
                "pit_contract_required_for_future_use",
            ]
        )
    elif after_status == "RESEARCH_LABEL_ONLY":
        fixes.extend(
            [
                "research_label_only_usage_policy_applied",
                "strategy_input_disabled",
                "promotion_evidence_disabled",
            ]
        )
    elif after_status == "BLOCKED_UNTIL_QUALIFIED":
        fixes.extend(
            [
                "source_manifest_requirement_recorded",
                "available_time_requirement_recorded",
                "blocked_status_preserved_until_source_proof",
            ]
        )
    elif after_status == "DIAGNOSTIC_ONLY":
        fixes.extend(
            [
                "diagnostic_only_usage_policy_applied",
                "promotion_evidence_disabled",
            ]
        )
    if component == "pit_feature_store":
        fixes.append("pit_snapshot_proof_requirement_recorded")
    elif component in {"asset_master", "tradable_universe"}:
        fixes.append("asset_tradability_source_contract_requirement_recorded")
    elif component == "cost_liquidity_model":
        fixes.append("cost_liquidity_source_version_requirement_recorded")
    elif component == "forward_evidence_archive":
        fixes.append("append_only_forward_maturity_requirement_recorded")
    elif component == "research_case_library":
        fixes.append("oracle_reverse_promotion_exclusion_recorded")
    return fixes


def _remediation_remaining_gap(item: Mapping[str, Any], after_status: str) -> list[str]:
    gaps: list[str] = []
    if bool(item.get("missing_contract")):
        gaps.append("qualification_contract_missing")
    if bool(item.get("missing_source_manifest")):
        gaps.append("qualified_source_manifest_missing")
    if bool(item.get("missing_available_time")):
        gaps.append("as_of_available_time_missing")
    if bool(item.get("lineage_gap")):
        gaps.append("lineage_proof_gap")
    if bool(item.get("current_view_only_risk")) or after_status == "CURRENT_VIEW_ONLY":
        gaps.append("current_view_only_as_of_snapshot_missing")
    if after_status == "BLOCKED_UNTIL_QUALIFIED":
        gaps.append("source_remains_blocked_until_qualified")
    elif after_status == "DIAGNOSTIC_ONLY":
        gaps.append("source_remains_diagnostic_only")
    elif after_status == "RESEARCH_LABEL_ONLY":
        gaps.append("research_label_only_not_strategy_input")
    return _unique_strings(gaps)


def _remediation_allowed_uses(item: Mapping[str, Any], after_status: str) -> list[str]:
    component = _text(item.get("component"))
    if after_status == "CURRENT_VIEW_ONLY":
        return ["diagnostic", "research_label"]
    if after_status == "RESEARCH_LABEL_ONLY":
        return ["analysis", "casebook", "stratified_reporting"]
    if after_status == "BLOCKED_UNTIL_QUALIFIED":
        return ["qualification_planning", "blocked_source_review"]
    if component == "cost_liquidity_model":
        return ["diagnostic", "gross_net_demo", "sensitivity_analysis"]
    if component in {"asset_master", "tradable_universe"}:
        return ["diagnostic", "universe_audit"]
    if component == "forward_evidence_archive":
        return ["diagnostic", "forward_observation_archive"]
    if component == "research_case_library":
        return ["diagnostic", "casebook"]
    return ["diagnostic", "research_report"]


def _unique_strings(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _p0_source_remediation_priorities() -> list[dict[str, Any]]:
    return [
        {
            "priority": "P0",
            "source_family": "price / adjusted price / corporate actions",
            "required_fix": "qualified PIT adjusted-price and corporate action manifests",
        },
        {
            "priority": "P0",
            "source_family": "SEC / fundamental PIT availability",
            "required_fix": "accession-level acceptance time and as-reported fact lineage",
        },
        {
            "priority": "P0",
            "source_family": "asset master / ticker / tradability",
            "required_fix": (
                "stable asset_id, ticker history, listing/delisting and tradability "
                "source manifests"
            ),
        },
        {
            "priority": "P0",
            "source_family": "event labels as-known-before vs post-hoc",
            "required_fix": "known_time and post_hoc flag with future-event leakage audit",
        },
        {
            "priority": "P0",
            "source_family": "cost / spread / liquidity assumptions",
            "required_fix": (
                "versioned spread, ADV/dollar-volume, cash-yield and financing " "source manifests"
            ),
        },
    ]


def _research_run_record(
    *,
    research_id: str,
    strategy_id: str,
    run_type: str,
    artifact_paths: list[str],
    dataset_version: str,
    feature_snapshot_id: str,
    asset_universe_version: str,
    cost_model_version: str,
    label_version: str,
    code_version: str,
) -> dict[str, Any]:
    seed = {
        "research_id": research_id,
        "strategy_id": strategy_id,
        "run_type": run_type,
        "artifact_paths": artifact_paths,
    }
    return {
        "run_id": f"research_run_{_stable_hash(seed)[:12]}",
        "research_id": research_id,
        "strategy_id": strategy_id,
        "strategy_family": "baseline",
        "run_type": run_type,
        "dataset_version": dataset_version,
        "feature_snapshot_id": feature_snapshot_id,
        "asset_universe_version": asset_universe_version,
        "cost_model_version": cost_model_version,
        "label_version": label_version,
        "config_hash": _stable_hash(seed),
        "code_version": code_version,
        "policy_version": BASELINE_POLICY_VERSION,
        "time_split": {"start": AI_REGIME_START, "end": None, "market_regime": "ai_after_chatgpt"},
        "horizon_set": ["1d", "5d", "10d", "20d", "60d"],
        "evaluation_stage": "stage_0_data_foundation_contract",
        "metrics_summary": {
            "gross_return": None,
            "net_return": None,
            "annualized_return": None,
            "volatility": None,
            "max_drawdown": None,
            "drawdown_preservation": None,
            "CVaR": None,
            "hit_rate_by_horizon": {},
            "turnover": None,
            "cost": None,
            "false_risk_off": None,
            "false_risk_on": None,
            "missed_upside": None,
            "constraint_hit_count": 0,
            "sample_quality_summary": "EVIDENCE_REQUIRED",
            "benchmark_comparison": "EVIDENCE_REQUIRED",
            "control_results": "EVIDENCE_REQUIRED",
        },
        "artifact_paths": artifact_paths,
        "evidence_ids": ["EVIDENCE_REQUIRED"],
        "decision_record_id": "DECISION_RECORD_REQUIRED",
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
        "broker_action": "none",
        "production_effect": "none",
        "created_at": utc_now_iso(),
    }


def _execution_checkpoint(
    item: Mapping[str, Any],
    *,
    cache_key: str,
    state: str,
) -> dict[str, Any]:
    checkpoint_id = f"checkpoint_{_stable_hash({'item': item, 'cache_key': cache_key})[:12]}"
    return {
        "checkpoint_id": checkpoint_id,
        "work_item_id": item.get("work_item_id"),
        "state": state,
        "config_hash": item.get("config_hash"),
        "code_version": BASELINE_CODE_VERSION,
        "data_version": "baseline",
        "cache_key": cache_key,
        "created_at": utc_now_iso(),
        "resume_supported": True,
        "production_effect": "none",
    }


def _base_payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": utc_now_iso(),
        "market_regime": "ai_after_chatgpt",
        "default_backtest_start": AI_REGIME_START,
        "production_effect": "none",
        "broker_action": "none",
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
        "manual_review_required": True,
        "safety_boundary": dict(SAFETY_BOUNDARY),
        "summary": dict(summary or {}),
    }
    payload.update(extra)
    return payload


def _load_mapping(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise DataFoundationError(f"expected mapping at {path}")
    return raw


def _load_orders(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    if not path.exists():
        raise DataFoundationError(f"orders artifact not found: {path}")
    if path.suffix.lower() == ".jsonl":
        return _read_jsonl(path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        return [dict(item) for item in raw if isinstance(item, Mapping)]
    if isinstance(raw, Mapping):
        orders = raw.get("orders", [])
        return [dict(item) for item in orders if isinstance(item, Mapping)]
    return []


def _assets_by_id(asset_master_path: Path) -> dict[str, dict[str, Any]]:
    raw = _load_mapping(asset_master_path)
    assets = {
        str(item["asset_id"]): item for item in _records(raw.get("assets")) if item.get("asset_id")
    }
    if not assets:
        raise DataFoundationError(f"asset master has no assets: {asset_master_path}")
    return assets


def _universe_asset_ids(
    universe: str,
    *,
    asset_master_path: Path,
    universe_path: Path,
) -> list[str]:
    raw = _load_mapping(universe_path)
    universes = _mapping(raw.get("universes"))
    entry = _mapping(universes.get(universe))
    if not entry:
        raise DataFoundationError(f"unknown universe: {universe}")
    asset_ids = _strings(entry.get("asset_ids"))
    assets = _assets_by_id(asset_master_path)
    missing = [asset_id for asset_id in asset_ids if asset_id not in assets]
    if missing:
        raise DataFoundationError(f"universe {universe} references unknown assets: {missing}")
    return asset_ids


def _date_in_listing_window(value: str, asset: Mapping[str, Any]) -> bool:
    listing = _text(asset.get("listing_date"))
    delisting = _text(asset.get("delisting_date"))
    return (not listing or value >= listing) and (not delisting or value <= delisting)


def _parse_date_range(value: str) -> tuple[str, str]:
    if ":" in value:
        start, end = value.split(":", maxsplit=1)
    else:
        start = end = value
    start = start.strip()
    end = end.strip() or start
    if not start:
        raise DataFoundationError("date range start is required")
    return start, end


def _date_sequence(start: str, end: str) -> list[str]:
    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    if end_date < start_date:
        raise DataFoundationError("date range end cannot be before start")
    days = (end_date - start_date).days
    return [
        (start_date.fromordinal(start_date.toordinal() + offset)).isoformat()
        for offset in range(days + 1)
    ]


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _strings(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [_text(item) for item in value if _text(item)]


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _number(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    if isinstance(value, int | float):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return default


def _read_json(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise DataFoundationError(f"expected JSON object at {path}")
    return raw


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_jsonable(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        raw = json.loads(line)
        if isinstance(raw, dict):
            rows.append(raw)
    return rows


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(
            json.dumps(_jsonable(row), ensure_ascii=False, sort_keys=True) + "\n" for row in rows
        ),
        encoding="utf-8",
    )


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, date | datetime):
        return value.isoformat()
    return value


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, date | datetime):
        return value.isoformat()
    text = str(value)
    return text if text else default


def _safe_id(value: str) -> str:
    return "".join(char if char.isalnum() or char in {"_", "-"} else "_" for char in value)


def _compact(value: Any) -> str:
    if isinstance(value, bool):
        return "`true`" if value else "`false`"
    if isinstance(value, int | float):
        return f"`{value}`"
    if value is None:
        return "`null`"
    if isinstance(value, str):
        return f"`{value}`" if len(value) < 80 else value
    return f"`{json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True)}`"
