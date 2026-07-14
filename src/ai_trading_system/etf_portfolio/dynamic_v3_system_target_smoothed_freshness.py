from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as legacy
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_hardening as hardening
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_portfolio as target_core
from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_bootstrap as bootstrap,
)
from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_operations as operations,
)
from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_promotion as promotion,
)
from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    _file_bytes_match,
    _json_bytes,
)
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    _mapping,
    _read_json,
    _records,
    _stable_id,
    _text,
    _unique_dir,
)

PREFLIGHT_SNAPSHOT_SCHEMA = "smoothed_data_preflight_input_snapshot.v2"
LATEST_SNAPSHOT_SCHEMA = "smoothed_latest_emission_input_snapshot.v2"
EXPLAIN_SNAPSHOT_SCHEMA = "smoothed_blocked_explain_input_snapshot.v2"
REFRESH_SNAPSHOT_SCHEMA = "smoothed_refresh_plan_input_snapshot.v2"
RETRY_SNAPSHOT_SCHEMA = "smoothed_bootstrap_retry_input_snapshot.v2"

DEFAULT_MODEL_TARGET_DIR = target_core.DEFAULT_MODEL_TARGET_DIR
DEFAULT_PRICE_CACHE_PATH = legacy.DEFAULT_PRICE_CACHE_PATH
DEFAULT_RATES_CACHE_PATH = legacy.DEFAULT_RATES_CACHE_PATH
DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR = legacy.DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR
DEFAULT_SMOOTHED_LATEST_EMISSION_DIR = legacy.DEFAULT_SMOOTHED_LATEST_EMISSION_DIR
DEFAULT_SMOOTHED_BLOCKED_EXPLAIN_DIR = legacy.DEFAULT_SMOOTHED_BLOCKED_EXPLAIN_DIR
DEFAULT_SMOOTHED_REFRESH_PLAN_DIR = legacy.DEFAULT_SMOOTHED_REFRESH_PLAN_DIR
DEFAULT_SMOOTHED_BOOTSTRAP_RETRY_DIR = legacy.DEFAULT_SMOOTHED_BOOTSTRAP_RETRY_DIR
DEFAULT_SMOOTHED_DAILY_EMISSION_DIR = bootstrap.DEFAULT_SMOOTHED_DAILY_EMISSION_DIR
DEFAULT_SMOOTHED_OUTCOME_DUE_DIR = bootstrap.DEFAULT_SMOOTHED_OUTCOME_DUE_DIR
DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR = bootstrap.DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR
DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR = bootstrap.DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR
DEFAULT_SMOOTHED_FORWARD_BINDING_DIR = bootstrap.DEFAULT_SMOOTHED_FORWARD_BINDING_DIR
DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR = bootstrap.DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR
DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR = bootstrap.DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR
DEFAULT_SMOOTHED_EVENT_MONITOR_DIR = bootstrap.DEFAULT_SMOOTHED_EVENT_MONITOR_DIR
DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR = bootstrap.DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR
DEFAULT_SMOOTHED_SWITCH_READINESS_DIR = bootstrap.DEFAULT_SMOOTHED_SWITCH_READINESS_DIR
DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR = bootstrap.DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR
DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR = bootstrap.DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR
DEFAULT_SMOOTHED_FORWARD_WEEKLY_RUN_DIR = bootstrap.DEFAULT_SMOOTHED_FORWARD_WEEKLY_RUN_DIR

SYSTEM_TARGET_SAFETY = promotion.SYSTEM_TARGET_SAFETY


class DynamicV3SmoothedFreshnessError(ValueError):
    """Raised when freshness/bootstrap evidence cannot be reproduced exactly."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3SmoothedFreshnessError(message)


def _generated_at(value: datetime | None) -> datetime:
    try:
        return target_core._generated_at(value)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3SmoothedFreshnessError(str(exc)) from exc


def _write(root: Path, views: Mapping[str, bytes], pointer: str, manifest: str) -> None:
    operations._write(root, views, pointer, manifest)


def _view_errors(root: Path, views: Mapping[str, bytes]) -> list[str]:
    return [name for name, payload in views.items() if not _file_bytes_match(root / name, payload)]


def _artifact_root(
    output_dir: Path,
    artifact_id: str | None,
    latest: bool,
    pointer: str,
) -> Path:
    return hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=artifact_id if not latest else None,
        pointer_name=pointer,
    )


def _validation_payload(
    report_type: str,
    artifact_id: str,
    errors: Sequence[str],
    mismatches: Sequence[str],
    *,
    artifact_id_key: str,
) -> dict[str, Any]:
    return operations._validation_payload(
        report_type,
        artifact_id,
        [
            legacy._check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            legacy._check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key=artifact_id_key,
    )


def _safe(*payloads: Mapping[str, Any]) -> bool:
    return all(
        payload.get("broker_action_allowed") is False
        and payload.get("not_official_target_weights") is True
        and payload.get("production_effect") == "none"
        for payload in payloads
    )


def _iso_date(value: Any, *, field: str) -> date:
    try:
        return date.fromisoformat(_text(value))
    except (TypeError, ValueError) as exc:
        raise DynamicV3SmoothedFreshnessError(f"{field} must be ISO date") from exc


def _texts(value: Any) -> list[str]:
    return [str(item) for item in value] if isinstance(value, list) else []


def _integer(value: Any, *, field: str) -> int:
    _require(isinstance(value, int) and not isinstance(value, bool), f"{field} must be integer")
    return int(value)


def _file_summary(summary: Any) -> dict[str, Any] | None:
    if summary is None:
        return None
    return {
        "path": str(summary.path),
        "exists": bool(summary.exists),
        "rows": int(summary.rows),
        "sha256": summary.sha256,
        "min_date": summary.min_date.isoformat() if summary.min_date else None,
        "max_date": summary.max_date.isoformat() if summary.max_date else None,
    }


def _strict_model_target_source(root: Path, generated: datetime) -> dict[str, Any] | None:
    manifests = sorted(root.glob("*/model_target_manifest.json")) if root.exists() else []
    if not manifests:
        return None
    candidates: list[tuple[date, datetime, Path]] = []
    for manifest_path in manifests:
        manifest = _read_json(manifest_path)
        as_of = target_core._date(manifest.get("as_of"), field="model target as_of")
        source_generated = target_core._datetime(
            manifest.get("generated_at"), field="model target generated_at"
        )
        _require(
            manifest.get("target_id") == manifest_path.parent.name,
            "model target artifact id mismatch",
        )
        if as_of <= generated.date() and source_generated <= generated:
            candidates.append((as_of, source_generated, manifest_path.parent))
    _require(bool(candidates), "no model target at or before preflight cutoff")
    latest_as_of = max(item[0] for item in candidates)
    selected = [item for item in candidates if item[0] == latest_as_of]
    _require(len(selected) == 1, f"ambiguous model target as_of: {latest_as_of}")
    return bootstrap._model_target_source(selected[0][2].name, root)


def _quality_payload(report: Any) -> dict[str, Any]:
    return {
        "status": report.status,
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "price_summary": _file_summary(report.price_summary),
        "rate_summary": _file_summary(report.rate_summary),
        "secondary_price_summary": _file_summary(report.secondary_price_summary),
        "manifest_summary": _file_summary(report.manifest_summary),
        "expected_price_tickers": list(report.expected_price_tickers),
        "expected_rate_series": list(report.expected_rate_series),
        "issues": legacy._smoothed_quality_issues_payload(report),
    }


def _preflight_input(
    *,
    requested_as_of: date | None,
    requested_week_ending: date | None,
    price_cache_path: Path,
    rates_path: Path,
    model_target_dir: Path,
    generated: datetime,
) -> dict[str, Any]:
    requested = legacy._smoothed_requested_date(
        requested_as_of=requested_as_of,
        requested_week_ending=requested_week_ending,
    )
    quality = legacy._smoothed_preflight_data_quality_report(
        prices_path=price_cache_path,
        rates_path=rates_path,
        as_of=requested,
    )
    return {
        "schema_version": PREFLIGHT_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "requested_as_of": requested_as_of.isoformat() if requested_as_of else None,
        "requested_week_ending": (
            requested_week_ending.isoformat() if requested_week_ending else None
        ),
        "requested_date": requested.isoformat(),
        "price_cache_path": str(price_cache_path),
        "rates_path": str(rates_path),
        "model_target_dir": str(model_target_dir),
        "quality_report": _quality_payload(quality),
        "model_target_source": _strict_model_target_source(model_target_dir, generated),
        "future_data_used": False,
        **SYSTEM_TARGET_SAFETY,
    }


def _preflight_business(
    snapshot: Mapping[str, Any], *, preflight_id: str
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    quality = _mapping(snapshot.get("quality_report"))
    prices = _mapping(quality.get("price_summary"))
    rates = _mapping(quality.get("rate_summary"))
    secondary = _mapping(quality.get("secondary_price_summary"))
    price_latest = prices.get("max_date")
    rate_latest = rates.get("max_date")
    secondary_latest = secondary.get("max_date") or None
    require_secondary = legacy._smoothed_requires_marketstack_prices(
        Path(_text(snapshot.get("price_cache_path")))
    )
    required = [price_latest, rate_latest]
    if require_secondary:
        required.append(secondary_latest)
    latest_valid = (
        min(_iso_date(item, field="latest source date") for item in required)
        if required and all(required)
        else None
    )
    issues = _records(quality.get("issues"))
    errors = list(
        dict.fromkeys(_text(row.get("code")) for row in issues if row.get("severity") == "ERROR")
    )
    warnings = list(
        dict.fromkeys(_text(row.get("code")) for row in issues if row.get("severity") == "WARNING")
    )
    requested = _iso_date(snapshot.get("requested_date"), field="requested date")
    status = legacy._smoothed_freshness_status(
        requested_date=requested,
        latest_valid_as_of=latest_valid,
        validate_data_status=_text(quality.get("status")),
        blocking_errors=errors,
    )
    target_source = _mapping(snapshot.get("model_target_source"))
    target_manifest = (
        hardening._bundle_json(target_source, "model_target_manifest.json") if target_source else {}
    )
    business = {
        "schema_version": 1,
        "preflight_id": preflight_id,
        "requested_as_of": snapshot.get("requested_as_of"),
        "requested_week_ending": snapshot.get("requested_week_ending"),
        "requested_date": snapshot.get("requested_date"),
        "latest_available": {
            "prices_daily": price_latest,
            "prices_marketstack_daily": secondary_latest,
            "rates_daily": rate_latest,
            "model_target_available_as_of": target_manifest.get("as_of"),
            "regime_tags_available_as_of": price_latest,
        },
        "latest_valid_as_of": latest_valid.isoformat() if latest_valid else None,
        "freshness_status": status,
        "validate_data_status": quality.get("status"),
        "blocking_errors": errors,
        "warnings": warnings,
        "quality_issues": issues,
        "source_paths": {
            "prices_daily": snapshot.get("price_cache_path"),
            "prices_marketstack_daily": str(
                legacy._smoothed_marketstack_prices_path(
                    Path(_text(snapshot.get("price_cache_path")))
                )
            ),
            "rates_daily": snapshot.get("rates_path"),
        },
        "future_data_used": False,
        **SYSTEM_TARGET_SAFETY,
    }
    commands = legacy._smoothed_runnable_command_matrix(business)
    commands["preflight_id"] = preflight_id
    commands.update(SYSTEM_TARGET_SAFETY)
    blocked = legacy._smoothed_blocked_reason_matrix(business)
    blocked["preflight_id"] = preflight_id
    return business, commands, blocked


def _preflight_views(
    snapshot: Mapping[str, Any], *, preflight_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    business, commands, blocked = _preflight_business(snapshot, preflight_id=preflight_id)
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_data_preflight_manifest",
        "preflight_id": preflight_id,
        "requested_as_of": snapshot.get("requested_as_of"),
        "requested_week_ending": snapshot.get("requested_week_ending"),
        "latest_valid_as_of": business.get("latest_valid_as_of"),
        "freshness_status": business.get("freshness_status"),
        "validate_data_status": business.get("validate_data_status"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "smoothed_data_preflight_input_snapshot_path": str(
            root / "smoothed_data_preflight_input_snapshot.json"
        ),
        "smoothed_data_preflight_manifest_path": str(
            root / "smoothed_data_preflight_manifest.json"
        ),
        "data_freshness_snapshot_path": str(root / "data_freshness_snapshot.json"),
        "runnable_command_matrix_path": str(root / "runnable_command_matrix.json"),
        "blocked_reason_matrix_path": str(root / "blocked_reason_matrix.json"),
        "smoothed_data_preflight_report_path": str(root / "smoothed_data_preflight_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = legacy.render_smoothed_data_preflight_report(manifest, business, commands, blocked)
    reader = legacy.render_smoothed_data_preflight_reader_brief(business, commands)
    views = {
        "smoothed_data_preflight_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_data_preflight_manifest.json": _json_bytes(manifest),
        "data_freshness_snapshot.json": _json_bytes(business),
        "runnable_command_matrix.json": _json_bytes(commands),
        "blocked_reason_matrix.json": _json_bytes(blocked),
        "smoothed_data_preflight_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "data_freshness_snapshot": business,
        "runnable_command_matrix": commands,
        "blocked_reason_matrix": blocked,
        "reader_brief_section": reader,
    }


def _validate_preflight_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == PREFLIGHT_SNAPSHOT_SCHEMA,
            "preflight snapshot schema invalid",
        )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="preflight generated_at"
        )
        requested_as_of = (
            _iso_date(snapshot.get("requested_as_of"), field="requested_as_of")
            if snapshot.get("requested_as_of")
            else None
        )
        requested_week_ending = (
            _iso_date(snapshot.get("requested_week_ending"), field="requested_week_ending")
            if snapshot.get("requested_week_ending")
            else None
        )
        rebuilt = _preflight_input(
            requested_as_of=requested_as_of,
            requested_week_ending=requested_week_ending,
            price_cache_path=Path(_text(snapshot.get("price_cache_path"))),
            rates_path=Path(_text(snapshot.get("rates_path"))),
            model_target_dir=Path(_text(snapshot.get("model_target_dir"))),
            generated=generated,
        )
        _require(rebuilt == dict(snapshot), "preflight live input drift")
        _require(
            snapshot.get("future_data_used") is False, "preflight future data boundary invalid"
        )
        _require(
            snapshot.get("production_effect") == "none", "preflight production boundary invalid"
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@promotion._with_validation_session
def run_smoothed_data_preflight(
    *,
    requested_as_of: date | None = None,
    requested_week_ending: date | None = None,
    output_dir: Path = DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
    price_cache_path: Path = DEFAULT_PRICE_CACHE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    model_target_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = _preflight_input(
        requested_as_of=requested_as_of,
        requested_week_ending=requested_week_ending,
        price_cache_path=price_cache_path,
        rates_path=rates_path,
        model_target_dir=model_target_dir,
        generated=generated,
    )
    errors = _validate_preflight_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-data-preflight", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _preflight_views(snapshot, preflight_id=root.name, root=root)
    _write(root, views, "latest_smoothed_data_preflight", "smoothed_data_preflight_manifest.json")
    return {"preflight_id": root.name, "preflight_dir": root, **payload}


def smoothed_data_preflight_report_payload(
    *,
    preflight_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
) -> dict[str, Any]:
    root = _artifact_root(output_dir, preflight_id, latest, "latest_smoothed_data_preflight")
    return {
        **_read_json(root / "smoothed_data_preflight_manifest.json"),
        "data_freshness_snapshot": _read_json(root / "data_freshness_snapshot.json"),
        "runnable_command_matrix": _read_json(root / "runnable_command_matrix.json"),
        "blocked_reason_matrix": _read_json(root / "blocked_reason_matrix.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "smoothed_data_preflight_input_snapshot.json"),
        "preflight_dir": str(root),
    }


@promotion._with_validation_session
def validate_smoothed_data_preflight_artifact(
    *,
    preflight_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
) -> dict[str, Any]:
    root = output_dir / preflight_id
    snapshot = (
        legacy._read_optional_json(root / "smoothed_data_preflight_input_snapshot.json") or {}
    )
    errors = _validate_preflight_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _preflight_views(snapshot, preflight_id=preflight_id, root=root)
        mismatches = _view_errors(root, views)
        _require(
            _safe(
                _mapping(payload.get("manifest")),
                _mapping(payload.get("data_freshness_snapshot")),
                _mapping(payload.get("runnable_command_matrix")),
                _mapping(payload.get("blocked_reason_matrix")),
            ),
            "preflight safety fields invalid",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_data_preflight_validation",
        preflight_id,
        errors,
        mismatches,
        artifact_id_key="preflight_id",
    )


def _preflight_source(preflight_id: str, root: Path) -> dict[str, Any]:
    return operations._local_source_binding(
        kind="smoothed_data_preflight",
        artifact_id=preflight_id,
        root=root,
        validator=validate_smoothed_data_preflight_artifact,
        validator_key="preflight_id",
        json_views=(
            "smoothed_data_preflight_manifest.json",
            "data_freshness_snapshot.json",
            "runnable_command_matrix.json",
            "blocked_reason_matrix.json",
        ),
        text_views=("smoothed_data_preflight_report.md", "reader_brief_section.md"),
    )


def _validate_preflight_source(source: Mapping[str, Any]) -> list[str]:
    return operations._validate_local_binding(
        source,
        kind="smoothed_data_preflight",
        validator=validate_smoothed_data_preflight_artifact,
        validator_key="preflight_id",
    )


def _bundle_json(source: Mapping[str, Any], name: str) -> dict[str, Any]:
    return hardening._bundle_json(source, name)


def _source_before_consumer(
    generated: datetime, source: Mapping[str, Any], manifest_name: str
) -> None:
    manifest = _bundle_json(source, manifest_name)
    source_generated = target_core._datetime(
        manifest.get("generated_at"), field=f"{manifest_name} generated_at"
    )
    _require(source_generated <= generated, f"{manifest_name} generated after consumer")


def _latest_input(
    *,
    preflight_id: str,
    preflight_dir: Path,
    model_target_dir: Path,
    emission_dir: Path,
    price_cache_path: Path,
    generated: datetime,
) -> dict[str, Any]:
    preflight_source = _preflight_source(preflight_id, preflight_dir)
    preflight = _bundle_json(preflight_source, "data_freshness_snapshot.json")
    preflight_snapshot = _read_json(
        Path(_text(_mapping(preflight_source.get("bundle")).get("source_dir")))
        / "smoothed_data_preflight_input_snapshot.json"
    )
    expected_price = Path(_text(preflight_snapshot.get("price_cache_path")))
    _require(
        expected_price.resolve() == price_cache_path.resolve(),
        "latest emission price cache differs from validated preflight",
    )
    requested = _iso_date(
        preflight.get("requested_as_of") or preflight.get("requested_week_ending"),
        field="preflight requested date",
    )
    resolved = _iso_date(preflight.get("latest_valid_as_of"), field="latest_valid_as_of")
    if resolved > requested:
        resolved = requested
    target_source = _mapping(preflight_snapshot.get("model_target_source"))
    target_id = _text(target_source.get("artifact_id")) or None
    if target_source:
        target_source_dir = Path(_text(_mapping(target_source.get("bundle")).get("source_dir")))
        _require(
            target_source_dir.parent.resolve() == model_target_dir.resolve(),
            "latest emission model target root differs from validated preflight",
        )
    emission = bootstrap.run_smoothed_daily_emission(
        as_of=resolved,
        target_id=target_id,
        model_target_dir=model_target_dir,
        output_dir=emission_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=Path(_text(preflight_snapshot.get("rates_path"))),
        generated_at=generated,
    )
    return {
        "schema_version": LATEST_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "requested_as_of": requested.isoformat(),
        "resolved_as_of": resolved.isoformat(),
        "preflight_source": preflight_source,
        "daily_emission_source": bootstrap._emission_source(emission["emission_id"], emission_dir),
        **SYSTEM_TARGET_SAFETY,
    }


def _latest_views(
    snapshot: Mapping[str, Any], *, latest_emission_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    preflight_source = _mapping(snapshot.get("preflight_source"))
    emission_source = _mapping(snapshot.get("daily_emission_source"))
    preflight = _bundle_json(preflight_source, "data_freshness_snapshot.json")
    emission = _bundle_json(emission_source, "smoothed_daily_emission_manifest.json")
    quality = _bundle_json(emission_source, "smoothed_emission_data_quality.json")
    requested = _text(snapshot.get("requested_as_of"))
    resolved = _text(snapshot.get("resolved_as_of"))
    resolution = {
        "schema_version": 2,
        "latest_emission_id": latest_emission_id,
        "source_preflight_id": preflight_source.get("artifact_id"),
        "requested_as_of": requested,
        "resolved_as_of": resolved,
        "resolution_reason": (
            "latest_valid_as_of_fallback" if resolved != requested else "requested_as_of_supported"
        ),
        "fallback_scope": "daily_emission_only",
        "due_scan_allowed": False,
        "outcome_update_allowed": False,
        "future_data_used": False,
        "data_quality": quality.get("data_quality"),
        "source_preflight_freshness_status": preflight.get("freshness_status"),
        "source_preflight_validate_data_status": preflight.get("validate_data_status"),
        "emission_status": emission.get("event_status") or "NOT_REGISTERED",
        **SYSTEM_TARGET_SAFETY,
    }
    links = {
        "schema_version": 2,
        "latest_emission_id": latest_emission_id,
        "resolved_as_of": resolved,
        "daily_emission_id": emission_source.get("artifact_id"),
        "daily_emission_dir": _mapping(emission_source.get("bundle")).get("source_dir"),
        "emitted_event_count": emission.get("emitted_event_count"),
        "event_status": emission.get("event_status") or "NOT_REGISTERED",
        **SYSTEM_TARGET_SAFETY,
    }
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_latest_emission_manifest",
        "latest_emission_id": latest_emission_id,
        "source_preflight_id": preflight_source.get("artifact_id"),
        "requested_as_of": requested,
        "resolved_as_of": resolved,
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "smoothed_latest_emission_input_snapshot_path": str(
            root / "smoothed_latest_emission_input_snapshot.json"
        ),
        "smoothed_latest_emission_manifest_path": str(
            root / "smoothed_latest_emission_manifest.json"
        ),
        "latest_emission_resolution_path": str(root / "latest_emission_resolution.json"),
        "latest_emission_artifact_links_path": str(root / "latest_emission_artifact_links.json"),
        "smoothed_latest_emission_report_path": str(root / "smoothed_latest_emission_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = legacy.render_smoothed_latest_emission_report(manifest, resolution, links)
    reader = legacy.render_smoothed_latest_emission_reader_brief(resolution, links)
    views = {
        "smoothed_latest_emission_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_latest_emission_manifest.json": _json_bytes(manifest),
        "latest_emission_resolution.json": _json_bytes(resolution),
        "latest_emission_artifact_links.json": _json_bytes(links),
        "smoothed_latest_emission_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "latest_emission_resolution": resolution,
        "latest_emission_artifact_links": links,
        "reader_brief_section": reader,
    }


def _validate_latest_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(snapshot.get("schema_version") == LATEST_SNAPSHOT_SCHEMA, "latest schema invalid")
        preflight_source = _mapping(snapshot.get("preflight_source"))
        emission_source = _mapping(snapshot.get("daily_emission_source"))
        errors.extend(_validate_preflight_source(preflight_source))
        errors.extend(
            operations._validate_local_binding(
                emission_source,
                kind="smoothed_daily_emission",
                validator=bootstrap.validate_smoothed_daily_emission_artifact,
                validator_key="emission_id",
            )
        )
        preflight = _bundle_json(preflight_source, "data_freshness_snapshot.json")
        emission = _bundle_json(emission_source, "smoothed_daily_emission_manifest.json")
        generated = target_core._datetime(snapshot.get("generated_at"), field="latest generated_at")
        _source_before_consumer(
            generated, preflight_source, "smoothed_data_preflight_manifest.json"
        )
        _source_before_consumer(generated, emission_source, "smoothed_daily_emission_manifest.json")
        _require(
            snapshot.get("resolved_as_of") == emission.get("as_of"),
            "latest emission resolved date mismatch",
        )
        _require(
            _iso_date(snapshot.get("resolved_as_of"), field="resolved_as_of")
            <= _iso_date(snapshot.get("requested_as_of"), field="requested_as_of"),
            "latest emission uses future data",
        )
        _require(
            snapshot.get("requested_as_of")
            == (preflight.get("requested_as_of") or preflight.get("requested_week_ending")),
            "latest preflight requested date mismatch",
        )
        _require(snapshot.get("production_effect") == "none", "latest production boundary invalid")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@promotion._with_validation_session
def run_smoothed_latest_emission(
    *,
    preflight_id: str,
    preflight_dir: Path = DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_LATEST_EMISSION_DIR,
    model_target_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    emission_dir: Path = DEFAULT_SMOOTHED_DAILY_EMISSION_DIR,
    price_cache_path: Path = DEFAULT_PRICE_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = _latest_input(
        preflight_id=preflight_id,
        preflight_dir=preflight_dir,
        model_target_dir=model_target_dir,
        emission_dir=emission_dir,
        price_cache_path=price_cache_path,
        generated=generated,
    )
    errors = _validate_latest_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-latest-emission", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _latest_views(snapshot, latest_emission_id=root.name, root=root)
    _write(root, views, "latest_smoothed_latest_emission", "smoothed_latest_emission_manifest.json")
    return {"latest_emission_id": root.name, "latest_emission_dir": root, **payload}


def smoothed_latest_emission_report_payload(
    *,
    latest_emission_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_LATEST_EMISSION_DIR,
) -> dict[str, Any]:
    root = _artifact_root(output_dir, latest_emission_id, latest, "latest_smoothed_latest_emission")
    return {
        **_read_json(root / "smoothed_latest_emission_manifest.json"),
        "latest_emission_resolution": _read_json(root / "latest_emission_resolution.json"),
        "latest_emission_artifact_links": _read_json(root / "latest_emission_artifact_links.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "smoothed_latest_emission_input_snapshot.json"),
        "latest_emission_dir": str(root),
    }


@promotion._with_validation_session
def validate_smoothed_latest_emission_artifact(
    *,
    latest_emission_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_LATEST_EMISSION_DIR,
) -> dict[str, Any]:
    root = output_dir / latest_emission_id
    snapshot = (
        legacy._read_optional_json(root / "smoothed_latest_emission_input_snapshot.json") or {}
    )
    errors = _validate_latest_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _latest_views(snapshot, latest_emission_id=latest_emission_id, root=root)
        mismatches = _view_errors(root, views)
        _require(
            _safe(
                _mapping(payload.get("manifest")),
                _mapping(payload.get("latest_emission_resolution")),
                _mapping(payload.get("latest_emission_artifact_links")),
            ),
            "latest emission safety fields invalid",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_latest_emission_validation",
        latest_emission_id,
        errors,
        mismatches,
        artifact_id_key="latest_emission_id",
    )


def _latest_source(latest_emission_id: str, root: Path) -> dict[str, Any]:
    return operations._local_source_binding(
        kind="smoothed_latest_emission",
        artifact_id=latest_emission_id,
        root=root,
        validator=validate_smoothed_latest_emission_artifact,
        validator_key="latest_emission_id",
        json_views=(
            "smoothed_latest_emission_manifest.json",
            "latest_emission_resolution.json",
            "latest_emission_artifact_links.json",
        ),
        text_views=("smoothed_latest_emission_report.md", "reader_brief_section.md"),
    )


def _explain_views(
    snapshot: Mapping[str, Any], *, explain_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    source = _mapping(snapshot.get("preflight_source"))
    freshness = _bundle_json(source, "data_freshness_snapshot.json")
    commands = _bundle_json(source, "runnable_command_matrix.json")
    explanations = legacy._smoothed_blocked_command_explanations(freshness, commands)
    payload = {
        "schema_version": 2,
        "explain_id": explain_id,
        "source_preflight_id": source.get("artifact_id"),
        "blocked_commands": explanations,
        **SYSTEM_TARGET_SAFETY,
    }
    owner = legacy.render_smoothed_blocked_owner_summary(freshness, explanations)
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_blocked_explain_manifest",
        "explain_id": explain_id,
        "source_preflight_id": source.get("artifact_id"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "blocked_command_count": len(explanations),
        "smoothed_blocked_explain_input_snapshot_path": str(
            root / "smoothed_blocked_explain_input_snapshot.json"
        ),
        "smoothed_blocked_explain_manifest_path": str(
            root / "smoothed_blocked_explain_manifest.json"
        ),
        "blocked_command_explanations_path": str(root / "blocked_command_explanations.json"),
        "blocked_owner_summary_path": str(root / "blocked_owner_summary.md"),
        "smoothed_blocked_explain_report_path": str(root / "smoothed_blocked_explain_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = legacy.render_smoothed_blocked_explain_report(manifest, freshness, explanations)
    reader = legacy.render_smoothed_blocked_explain_reader_brief(freshness, explanations)
    views = {
        "smoothed_blocked_explain_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_blocked_explain_manifest.json": _json_bytes(manifest),
        "blocked_command_explanations.json": _json_bytes(payload),
        "blocked_owner_summary.md": owner.encode("utf-8"),
        "smoothed_blocked_explain_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "blocked_command_explanations": payload,
        "blocked_owner_summary": owner,
        "reader_brief_section": reader,
    }


def _validate_explain_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == EXPLAIN_SNAPSHOT_SCHEMA, "explain schema invalid"
        )
        source = _mapping(snapshot.get("preflight_source"))
        errors.extend(_validate_preflight_source(source))
        _source_before_consumer(
            target_core._datetime(snapshot.get("generated_at"), field="explain generated_at"),
            source,
            "smoothed_data_preflight_manifest.json",
        )
        _require(snapshot.get("production_effect") == "none", "explain production boundary invalid")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@promotion._with_validation_session
def run_smoothed_blocked_explain(
    *,
    preflight_id: str,
    preflight_dir: Path = DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_BLOCKED_EXPLAIN_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    snapshot = {
        "schema_version": EXPLAIN_SNAPSHOT_SCHEMA,
        "generated_at": _generated_at(generated_at).isoformat(),
        "preflight_source": _preflight_source(preflight_id, preflight_dir),
        **SYSTEM_TARGET_SAFETY,
    }
    errors = _validate_explain_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-blocked-explain", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _explain_views(snapshot, explain_id=root.name, root=root)
    _write(root, views, "latest_smoothed_blocked_explain", "smoothed_blocked_explain_manifest.json")
    return {"explain_id": root.name, "explain_dir": root, **payload}


def smoothed_blocked_explain_report_payload(
    *,
    explain_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_BLOCKED_EXPLAIN_DIR,
) -> dict[str, Any]:
    root = _artifact_root(output_dir, explain_id, latest, "latest_smoothed_blocked_explain")
    return {
        **_read_json(root / "smoothed_blocked_explain_manifest.json"),
        "blocked_command_explanations": _read_json(root / "blocked_command_explanations.json"),
        "blocked_owner_summary": (root / "blocked_owner_summary.md").read_text(encoding="utf-8"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "smoothed_blocked_explain_input_snapshot.json"),
        "explain_dir": str(root),
    }


@promotion._with_validation_session
def validate_smoothed_blocked_explain_artifact(
    *,
    explain_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_BLOCKED_EXPLAIN_DIR,
) -> dict[str, Any]:
    root = output_dir / explain_id
    snapshot = (
        legacy._read_optional_json(root / "smoothed_blocked_explain_input_snapshot.json") or {}
    )
    errors = _validate_explain_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _explain_views(snapshot, explain_id=explain_id, root=root)
        mismatches = _view_errors(root, views)
        _require(
            _safe(
                _mapping(payload.get("manifest")),
                _mapping(payload.get("blocked_command_explanations")),
            ),
            "explain safety fields invalid",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_blocked_explain_validation",
        explain_id,
        errors,
        mismatches,
        artifact_id_key="explain_id",
    )


def _explain_source(explain_id: str, root: Path) -> dict[str, Any]:
    return operations._local_source_binding(
        kind="smoothed_blocked_explain",
        artifact_id=explain_id,
        root=root,
        validator=validate_smoothed_blocked_explain_artifact,
        validator_key="explain_id",
        json_views=("smoothed_blocked_explain_manifest.json", "blocked_command_explanations.json"),
        text_views=(
            "blocked_owner_summary.md",
            "smoothed_blocked_explain_report.md",
            "reader_brief_section.md",
        ),
    )


def _refresh_views(
    snapshot: Mapping[str, Any], *, refresh_plan_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    preflight_source = _mapping(snapshot.get("preflight_source"))
    explain_source = _mapping(snapshot.get("explain_source"))
    freshness = _bundle_json(preflight_source, "data_freshness_snapshot.json")
    explain_manifest = _bundle_json(explain_source, "smoothed_blocked_explain_manifest.json")
    requirements = legacy._smoothed_source_refresh_requirements(freshness)
    rerun = legacy._smoothed_rerun_command_plan(freshness)
    requirements["refresh_plan_id"] = refresh_plan_id
    rerun["refresh_plan_id"] = refresh_plan_id
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_refresh_plan_manifest",
        "refresh_plan_id": refresh_plan_id,
        "source_preflight_id": preflight_source.get("artifact_id"),
        "source_explain_id": explain_source.get("artifact_id"),
        "requested_as_of": freshness.get("requested_as_of")
        or freshness.get("requested_week_ending"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "smoothed_refresh_plan_input_snapshot_path": str(
            root / "smoothed_refresh_plan_input_snapshot.json"
        ),
        "smoothed_refresh_plan_manifest_path": str(root / "smoothed_refresh_plan_manifest.json"),
        "source_refresh_requirements_path": str(root / "source_refresh_requirements.json"),
        "rerun_command_plan_path": str(root / "rerun_command_plan.json"),
        "smoothed_refresh_plan_report_path": str(root / "smoothed_refresh_plan_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = legacy.render_smoothed_refresh_plan_report(
        manifest, requirements, rerun, explain_manifest
    )
    reader = legacy.render_smoothed_refresh_plan_reader_brief(requirements, rerun)
    views = {
        "smoothed_refresh_plan_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_refresh_plan_manifest.json": _json_bytes(manifest),
        "source_refresh_requirements.json": _json_bytes(requirements),
        "rerun_command_plan.json": _json_bytes(rerun),
        "smoothed_refresh_plan_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "source_refresh_requirements": requirements,
        "rerun_command_plan": rerun,
        "reader_brief_section": reader,
    }


def _validate_refresh_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == REFRESH_SNAPSHOT_SCHEMA, "refresh schema invalid"
        )
        preflight_source = _mapping(snapshot.get("preflight_source"))
        explain_source = _mapping(snapshot.get("explain_source"))
        errors.extend(_validate_preflight_source(preflight_source))
        errors.extend(
            operations._validate_local_binding(
                explain_source,
                kind="smoothed_blocked_explain",
                validator=validate_smoothed_blocked_explain_artifact,
                validator_key="explain_id",
            )
        )
        explain_manifest = _bundle_json(explain_source, "smoothed_blocked_explain_manifest.json")
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="refresh generated_at"
        )
        _source_before_consumer(
            generated, preflight_source, "smoothed_data_preflight_manifest.json"
        )
        _source_before_consumer(generated, explain_source, "smoothed_blocked_explain_manifest.json")
        _require(
            explain_manifest.get("source_preflight_id") == preflight_source.get("artifact_id"),
            "refresh explain/preflight lineage mismatch",
        )
        _require(snapshot.get("production_effect") == "none", "refresh production boundary invalid")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@promotion._with_validation_session
def run_smoothed_refresh_plan(
    *,
    preflight_id: str,
    explain_id: str,
    preflight_dir: Path = DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
    explain_dir: Path = DEFAULT_SMOOTHED_BLOCKED_EXPLAIN_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_REFRESH_PLAN_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    snapshot = {
        "schema_version": REFRESH_SNAPSHOT_SCHEMA,
        "generated_at": _generated_at(generated_at).isoformat(),
        "preflight_source": _preflight_source(preflight_id, preflight_dir),
        "explain_source": _explain_source(explain_id, explain_dir),
        **SYSTEM_TARGET_SAFETY,
    }
    errors = _validate_refresh_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-refresh-plan", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _refresh_views(snapshot, refresh_plan_id=root.name, root=root)
    _write(root, views, "latest_smoothed_refresh_plan", "smoothed_refresh_plan_manifest.json")
    return {"refresh_plan_id": root.name, "refresh_plan_dir": root, **payload}


def smoothed_refresh_plan_report_payload(
    *,
    refresh_plan_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_REFRESH_PLAN_DIR,
) -> dict[str, Any]:
    root = _artifact_root(output_dir, refresh_plan_id, latest, "latest_smoothed_refresh_plan")
    return {
        **_read_json(root / "smoothed_refresh_plan_manifest.json"),
        "source_refresh_requirements": _read_json(root / "source_refresh_requirements.json"),
        "rerun_command_plan": _read_json(root / "rerun_command_plan.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "smoothed_refresh_plan_input_snapshot.json"),
        "refresh_plan_dir": str(root),
    }


@promotion._with_validation_session
def validate_smoothed_refresh_plan_artifact(
    *,
    refresh_plan_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_REFRESH_PLAN_DIR,
) -> dict[str, Any]:
    root = output_dir / refresh_plan_id
    snapshot = legacy._read_optional_json(root / "smoothed_refresh_plan_input_snapshot.json") or {}
    errors = _validate_refresh_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _refresh_views(snapshot, refresh_plan_id=refresh_plan_id, root=root)
        mismatches = _view_errors(root, views)
        _require(
            _safe(
                _mapping(payload.get("manifest")),
                _mapping(payload.get("source_refresh_requirements")),
                _mapping(payload.get("rerun_command_plan")),
            ),
            "refresh safety fields invalid",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_refresh_plan_validation",
        refresh_plan_id,
        errors,
        mismatches,
        artifact_id_key="refresh_plan_id",
    )


def _weekly_source(weekly_run_id: str, root: Path) -> dict[str, Any]:
    return operations._local_source_binding(
        kind="smoothed_forward_weekly_run",
        artifact_id=weekly_run_id,
        root=root,
        validator=bootstrap.validate_smoothed_forward_weekly_run_artifact,
        validator_key="weekly_run_id",
        json_views=(
            "smoothed_forward_weekly_run_manifest.json",
            "weekly_run_steps.json",
            "weekly_run_artifacts.json",
            "weekly_run_summary.json",
        ),
        text_views=("smoothed_forward_weekly_run_report.md", "reader_brief_section.md"),
    )


def _retry_child(
    *,
    status: str,
    requested: date,
    preflight_id: str,
    generated: datetime,
    paths: Mapping[str, Path],
    binding_id: str | None,
    switch_plan_id: str | None,
    owner_promotion_id: str | None,
) -> tuple[str | None, dict[str, Any] | None]:
    if status in {"READY", "READY_WITH_WARNINGS"}:
        weekly = bootstrap.run_smoothed_forward_weekly_run(
            week_ending=requested,
            binding_id=binding_id,
            switch_plan_id=switch_plan_id,
            owner_promotion_id=owner_promotion_id,
            model_target_dir=paths["model_target_dir"],
            emission_dir=paths["emission_dir"],
            due_dir=paths["due_dir"],
            update_dir=paths["update_dir"],
            classification_dir=paths["classification_dir"],
            binding_dir=paths["binding_dir"],
            progress_dir=paths["progress_dir"],
            dashboard_dir=paths["dashboard_dir"],
            monitor_dir=paths["monitor_dir"],
            switch_plan_dir=paths["switch_plan_dir"],
            recheck_dir=paths["recheck_dir"],
            owner_promotion_dir=paths["owner_promotion_dir"],
            renewal_dir=paths["renewal_dir"],
            output_dir=paths["weekly_run_dir"],
            price_cache_path=paths["price_cache_path"],
            rates_cache_path=paths["rates_path"],
            generated_at=generated,
        )
        return "weekly", _weekly_source(weekly["weekly_run_id"], paths["weekly_run_dir"])
    if status == "LATEST_AVAILABLE_ONLY":
        latest = run_smoothed_latest_emission(
            preflight_id=preflight_id,
            preflight_dir=paths["preflight_dir"],
            output_dir=paths["latest_emission_dir"],
            model_target_dir=paths["model_target_dir"],
            emission_dir=paths["emission_dir"],
            price_cache_path=paths["price_cache_path"],
            generated_at=generated,
        )
        return "latest", _latest_source(latest["latest_emission_id"], paths["latest_emission_dir"])
    return None, None


def _retry_views(
    snapshot: Mapping[str, Any], *, retry_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    preflight_source = _mapping(snapshot.get("preflight_source"))
    freshness = _bundle_json(preflight_source, "data_freshness_snapshot.json")
    status = _text(freshness.get("freshness_status"))
    can_full = status in {"READY", "READY_WITH_WARNINGS"}
    can_latest = status == "LATEST_AVAILABLE_ONLY"
    steps: list[dict[str, Any]] = [
        {
            "step": "preflight",
            "status": "PASS" if can_full or can_latest else "BLOCKED",
            "artifact_id": preflight_source.get("artifact_id"),
        }
    ]
    artifacts: dict[str, Any] = {"preflight": {"artifact_id": preflight_source.get("artifact_id")}}
    child_type = snapshot.get("child_type")
    child = _mapping(snapshot.get("child_source"))
    weekly_payload: dict[str, Any] | None = None
    latest_payload: dict[str, Any] | None = None
    if child_type == "weekly":
        weekly_steps = _bundle_json(child, "weekly_run_steps.json")
        weekly_artifacts = _bundle_json(child, "weekly_run_artifacts.json")
        weekly_summary = _bundle_json(child, "weekly_run_summary.json")
        steps.extend(_records(weekly_steps.get("steps")))
        artifacts["weekly_runner"] = {"artifact_id": child.get("artifact_id")}
        artifacts.update(_mapping(weekly_artifacts.get("artifacts")))
        weekly_payload = {"weekly_run_summary": weekly_summary}
    elif child_type == "latest":
        links = _bundle_json(child, "latest_emission_artifact_links.json")
        steps.append(
            {"step": "latest_emission", "status": "PASS", "artifact_id": child.get("artifact_id")}
        )
        artifacts["latest_emission"] = {"artifact_id": child.get("artifact_id")}
        legacy._append_retry_skipped_steps(
            steps,
            (
                "outcome_due_scan",
                "outcome_update",
                "forward_classification",
                "progress_update",
                "weekly_dashboard",
                "event_monitor",
                "switch_readiness",
                "owner_renewal",
            ),
            "latest_available_emission_only",
        )
        latest_payload = {"latest_emission_artifact_links": links}
    else:
        legacy._append_retry_skipped_steps(
            steps,
            (
                "daily_emission",
                "outcome_due_scan",
                "outcome_update",
                "forward_classification",
                "progress_update",
                "weekly_dashboard",
                "event_monitor",
                "switch_readiness",
                "owner_renewal",
            ),
            "preflight_blocked",
        )
    requested = _iso_date(snapshot.get("requested_date"), field="retry requested date")
    preflight_result = {
        "schema_version": 2,
        "retry_id": retry_id,
        "requested_as_of": requested.isoformat(),
        "preflight_status": status,
        "latest_valid_as_of": freshness.get("latest_valid_as_of"),
        "can_run_full_retry": can_full,
        "can_run_latest_available_emission_only": can_latest,
        "blocking_errors": _texts(freshness.get("blocking_errors")),
        **SYSTEM_TARGET_SAFETY,
    }
    step_payload = {"schema_version": 2, "steps": steps, **SYSTEM_TARGET_SAFETY}
    artifact_payload = {
        "schema_version": 2,
        "retry_id": retry_id,
        "artifacts": artifacts,
        **SYSTEM_TARGET_SAFETY,
    }
    summary = legacy._smoothed_retry_summary(
        retry_id=retry_id,
        requested_date=requested,
        preflight_status=status,
        weekly=weekly_payload,
        latest_emission=latest_payload,
    )
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_bootstrap_retry_manifest",
        "retry_id": retry_id,
        "requested_as_of": requested.isoformat(),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "retry_status": summary.get("retry_status"),
        "smoothed_bootstrap_retry_input_snapshot_path": str(
            root / "smoothed_bootstrap_retry_input_snapshot.json"
        ),
        "smoothed_bootstrap_retry_manifest_path": str(
            root / "smoothed_bootstrap_retry_manifest.json"
        ),
        "retry_preflight_result_path": str(root / "retry_preflight_result.json"),
        "retry_steps_path": str(root / "retry_steps.json"),
        "retry_artifacts_path": str(root / "retry_artifacts.json"),
        "retry_summary_path": str(root / "retry_summary.json"),
        "smoothed_bootstrap_retry_report_path": str(root / "smoothed_bootstrap_retry_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = legacy.render_smoothed_bootstrap_retry_report(
        manifest, preflight_result, step_payload, summary
    )
    reader = legacy.render_smoothed_bootstrap_retry_reader_brief(summary)
    views = {
        "smoothed_bootstrap_retry_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_bootstrap_retry_manifest.json": _json_bytes(manifest),
        "retry_preflight_result.json": _json_bytes(preflight_result),
        "retry_steps.json": _json_bytes(step_payload),
        "retry_artifacts.json": _json_bytes(artifact_payload),
        "retry_summary.json": _json_bytes(summary),
        "smoothed_bootstrap_retry_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "retry_preflight_result": preflight_result,
        "retry_steps": step_payload,
        "retry_artifacts": artifact_payload,
        "retry_summary": summary,
        "reader_brief_section": reader,
    }


def _validate_retry_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(snapshot.get("schema_version") == RETRY_SNAPSHOT_SCHEMA, "retry schema invalid")
        preflight_source = _mapping(snapshot.get("preflight_source"))
        errors.extend(_validate_preflight_source(preflight_source))
        freshness = _bundle_json(preflight_source, "data_freshness_snapshot.json")
        status = _text(freshness.get("freshness_status"))
        expected_child = (
            "weekly"
            if status in {"READY", "READY_WITH_WARNINGS"}
            else ("latest" if status == "LATEST_AVAILABLE_ONLY" else None)
        )
        _require(snapshot.get("child_type") == expected_child, "retry child branch mismatch")
        child = _mapping(snapshot.get("child_source"))
        generated = target_core._datetime(snapshot.get("generated_at"), field="retry generated_at")
        _source_before_consumer(
            generated, preflight_source, "smoothed_data_preflight_manifest.json"
        )
        if expected_child == "weekly":
            errors.extend(
                operations._validate_local_binding(
                    child,
                    kind="smoothed_forward_weekly_run",
                    validator=bootstrap.validate_smoothed_forward_weekly_run_artifact,
                    validator_key="weekly_run_id",
                )
            )
            weekly_manifest = _bundle_json(child, "smoothed_forward_weekly_run_manifest.json")
            _source_before_consumer(generated, child, "smoothed_forward_weekly_run_manifest.json")
            _require(
                weekly_manifest.get("week_ending") == snapshot.get("requested_date"),
                "retry weekly requested date mismatch",
            )
        elif expected_child == "latest":
            errors.extend(
                operations._validate_local_binding(
                    child,
                    kind="smoothed_latest_emission",
                    validator=validate_smoothed_latest_emission_artifact,
                    validator_key="latest_emission_id",
                )
            )
            latest_manifest = _bundle_json(child, "smoothed_latest_emission_manifest.json")
            _source_before_consumer(generated, child, "smoothed_latest_emission_manifest.json")
            _require(
                latest_manifest.get("source_preflight_id") == preflight_source.get("artifact_id"),
                "retry latest/preflight lineage mismatch",
            )
        else:
            _require(not child, "blocked retry must not bind a child artifact")
        _require(
            snapshot.get("requested_date") == freshness.get("requested_date"),
            "retry/preflight requested date mismatch",
        )
        _require(snapshot.get("production_effect") == "none", "retry production boundary invalid")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@promotion._with_validation_session
def run_smoothed_bootstrap_retry(
    *,
    requested_as_of: date | None = None,
    requested_week_ending: date | None = None,
    output_dir: Path = DEFAULT_SMOOTHED_BOOTSTRAP_RETRY_DIR,
    preflight_dir: Path = DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
    latest_emission_dir: Path = DEFAULT_SMOOTHED_LATEST_EMISSION_DIR,
    model_target_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    emission_dir: Path = DEFAULT_SMOOTHED_DAILY_EMISSION_DIR,
    due_dir: Path = DEFAULT_SMOOTHED_OUTCOME_DUE_DIR,
    update_dir: Path = DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR,
    classification_dir: Path = DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR,
    binding_dir: Path = DEFAULT_SMOOTHED_FORWARD_BINDING_DIR,
    progress_dir: Path = DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR,
    dashboard_dir: Path = DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR,
    monitor_dir: Path = DEFAULT_SMOOTHED_EVENT_MONITOR_DIR,
    switch_plan_dir: Path = DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR,
    recheck_dir: Path = DEFAULT_SMOOTHED_SWITCH_READINESS_DIR,
    owner_promotion_dir: Path = DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR,
    renewal_dir: Path = DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR,
    weekly_run_dir: Path = DEFAULT_SMOOTHED_FORWARD_WEEKLY_RUN_DIR,
    binding_id: str | None = None,
    switch_plan_id: str | None = None,
    owner_promotion_id: str | None = None,
    price_cache_path: Path = DEFAULT_PRICE_CACHE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    requested = legacy._smoothed_requested_date(
        requested_as_of=requested_as_of,
        requested_week_ending=requested_week_ending,
    )
    preflight = run_smoothed_data_preflight(
        requested_as_of=requested_as_of,
        requested_week_ending=requested_week_ending,
        output_dir=preflight_dir,
        price_cache_path=price_cache_path,
        rates_path=rates_path,
        model_target_dir=model_target_dir,
        generated_at=generated,
    )
    preflight_source = _preflight_source(preflight["preflight_id"], preflight_dir)
    status = _text(
        _bundle_json(preflight_source, "data_freshness_snapshot.json").get("freshness_status")
    )
    paths = {
        "preflight_dir": preflight_dir,
        "latest_emission_dir": latest_emission_dir,
        "model_target_dir": model_target_dir,
        "emission_dir": emission_dir,
        "due_dir": due_dir,
        "update_dir": update_dir,
        "classification_dir": classification_dir,
        "binding_dir": binding_dir,
        "progress_dir": progress_dir,
        "dashboard_dir": dashboard_dir,
        "monitor_dir": monitor_dir,
        "switch_plan_dir": switch_plan_dir,
        "recheck_dir": recheck_dir,
        "owner_promotion_dir": owner_promotion_dir,
        "renewal_dir": renewal_dir,
        "weekly_run_dir": weekly_run_dir,
        "price_cache_path": price_cache_path,
        "rates_path": rates_path,
    }
    child_type, child_source = _retry_child(
        status=status,
        requested=requested,
        preflight_id=preflight["preflight_id"],
        generated=generated,
        paths=paths,
        binding_id=binding_id,
        switch_plan_id=switch_plan_id,
        owner_promotion_id=owner_promotion_id,
    )
    snapshot = {
        "schema_version": RETRY_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "requested_as_of": requested_as_of.isoformat() if requested_as_of else None,
        "requested_week_ending": (
            requested_week_ending.isoformat() if requested_week_ending else None
        ),
        "requested_date": requested.isoformat(),
        "binding_id": binding_id,
        "switch_plan_id": switch_plan_id,
        "owner_promotion_id": owner_promotion_id,
        "preflight_source": preflight_source,
        "child_type": child_type,
        "child_source": child_source,
        **SYSTEM_TARGET_SAFETY,
    }
    errors = _validate_retry_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-bootstrap-retry", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _retry_views(snapshot, retry_id=root.name, root=root)
    _write(root, views, "latest_smoothed_bootstrap_retry", "smoothed_bootstrap_retry_manifest.json")
    latest_emission = (
        smoothed_latest_emission_report_payload(
            latest_emission_id=_text(_mapping(child_source).get("artifact_id")),
            output_dir=latest_emission_dir,
        )
        if child_type == "latest"
        else None
    )
    weekly_run = (
        bootstrap.smoothed_forward_weekly_run_report_payload(
            weekly_run_id=_text(_mapping(child_source).get("artifact_id")),
            output_dir=weekly_run_dir,
        )
        if child_type == "weekly"
        else None
    )
    return {
        "retry_id": root.name,
        "retry_dir": root,
        "preflight": preflight,
        "latest_emission": latest_emission,
        "weekly_run": weekly_run,
        **payload,
    }


def smoothed_bootstrap_retry_report_payload(
    *,
    retry_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_BOOTSTRAP_RETRY_DIR,
) -> dict[str, Any]:
    root = _artifact_root(output_dir, retry_id, latest, "latest_smoothed_bootstrap_retry")
    return {
        **_read_json(root / "smoothed_bootstrap_retry_manifest.json"),
        "retry_preflight_result": _read_json(root / "retry_preflight_result.json"),
        "retry_steps": _read_json(root / "retry_steps.json"),
        "retry_artifacts": _read_json(root / "retry_artifacts.json"),
        "retry_summary": _read_json(root / "retry_summary.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "smoothed_bootstrap_retry_input_snapshot.json"),
        "retry_dir": str(root),
    }


@promotion._with_validation_session
def validate_smoothed_bootstrap_retry_artifact(
    *,
    retry_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_BOOTSTRAP_RETRY_DIR,
) -> dict[str, Any]:
    root = output_dir / retry_id
    snapshot = (
        legacy._read_optional_json(root / "smoothed_bootstrap_retry_input_snapshot.json") or {}
    )
    errors = _validate_retry_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _retry_views(snapshot, retry_id=retry_id, root=root)
        mismatches = _view_errors(root, views)
        _require(
            _safe(
                _mapping(payload.get("manifest")),
                _mapping(payload.get("retry_preflight_result")),
                _mapping(payload.get("retry_steps")),
                _mapping(payload.get("retry_artifacts")),
                _mapping(payload.get("retry_summary")),
            ),
            "retry safety fields invalid",
        )
        _require(
            _mapping(payload.get("retry_summary")).get("can_execute_switch") is False,
            "retry switch execution enabled",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_bootstrap_retry_validation",
        retry_id,
        errors,
        mismatches,
        artifact_id_key="retry_id",
    )
