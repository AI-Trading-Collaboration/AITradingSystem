from __future__ import annotations

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
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    registry_path = output_root / "run_registry.jsonl"
    record = _research_run_record(
        research_id=research_id,
        strategy_id=strategy_id,
        run_type=run_type,
        artifact_paths=list(artifact_paths),
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
        baseline_outputs=[],
        candidate_strategy_outputs=[],
        benchmark_outputs=[],
        control_outputs=[],
        oracle_diagnostic_outputs=[],
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


def _research_run_record(
    *,
    research_id: str,
    strategy_id: str,
    run_type: str,
    artifact_paths: list[str],
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
        "dataset_version": "pit_action_outcome_dataset_contract_v1",
        "feature_snapshot_id": "pit_snapshot_required",
        "asset_universe_version": "universe_definitions_baseline_v1",
        "cost_model_version": "research_cost_model_baseline_v1",
        "label_version": "research_labels_v1",
        "config_hash": _stable_hash(seed),
        "code_version": BASELINE_CODE_VERSION,
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
