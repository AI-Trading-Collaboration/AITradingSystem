from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as legacy
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_hardening as hardening
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_portfolio as target_core
from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_freshness as freshness,
)
from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_operations as operations,
)
from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_promotion as promotion,
)
from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import _json_bytes
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    _mapping,
    _read_json,
    _records,
    _stable_id,
    _text,
    _unique_dir,
)

SOURCE_REFRESH_SNAPSHOT_SCHEMA = "smoothed_source_refresh_input_snapshot.v2"
POST_REFRESH_SNAPSHOT_SCHEMA = "smoothed_post_refresh_validation_input_snapshot.v2"
RETRY_RESUME_SNAPSHOT_SCHEMA = "smoothed_retry_resume_input_snapshot.v2"
SAMPLE_GROWTH_SNAPSHOT_SCHEMA = "smoothed_sample_growth_input_snapshot.v2"
DATA_READINESS_SNAPSHOT_SCHEMA = "smoothed_data_readiness_input_snapshot.v2"

DEFAULT_MODEL_TARGET_DIR = freshness.DEFAULT_MODEL_TARGET_DIR
DEFAULT_PRICE_CACHE_PATH = legacy.DEFAULT_PRICE_CACHE_PATH
DEFAULT_RATES_CACHE_PATH = legacy.DEFAULT_RATES_CACHE_PATH
DEFAULT_SMOOTHED_SOURCE_REFRESH_CONFIG_PATH = legacy.DEFAULT_SMOOTHED_SOURCE_REFRESH_CONFIG_PATH
DEFAULT_SMOOTHED_SOURCE_REFRESH_DIR = legacy.DEFAULT_SMOOTHED_SOURCE_REFRESH_DIR
DEFAULT_SMOOTHED_POST_REFRESH_VALIDATION_DIR = (
    legacy.DEFAULT_SMOOTHED_POST_REFRESH_VALIDATION_DIR
)
DEFAULT_SMOOTHED_RETRY_RESUME_DIR = legacy.DEFAULT_SMOOTHED_RETRY_RESUME_DIR
DEFAULT_SMOOTHED_SAMPLE_GROWTH_DIR = legacy.DEFAULT_SMOOTHED_SAMPLE_GROWTH_DIR
DEFAULT_SMOOTHED_DATA_READINESS_DIR = legacy.DEFAULT_SMOOTHED_DATA_READINESS_DIR
DEFAULT_SMOOTHED_REFRESH_PLAN_DIR = freshness.DEFAULT_SMOOTHED_REFRESH_PLAN_DIR
DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR = freshness.DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR
DEFAULT_SMOOTHED_BOOTSTRAP_RETRY_DIR = freshness.DEFAULT_SMOOTHED_BOOTSTRAP_RETRY_DIR
DEFAULT_SMOOTHED_LATEST_EMISSION_DIR = freshness.DEFAULT_SMOOTHED_LATEST_EMISSION_DIR
DEFAULT_SMOOTHED_DAILY_EMISSION_DIR = freshness.DEFAULT_SMOOTHED_DAILY_EMISSION_DIR
DEFAULT_SMOOTHED_OUTCOME_DUE_DIR = freshness.DEFAULT_SMOOTHED_OUTCOME_DUE_DIR
DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR = freshness.DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR
DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR = freshness.DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR
DEFAULT_SMOOTHED_FORWARD_BINDING_DIR = freshness.DEFAULT_SMOOTHED_FORWARD_BINDING_DIR
DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR = freshness.DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR
DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR = freshness.DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR
DEFAULT_SMOOTHED_EVENT_MONITOR_DIR = freshness.DEFAULT_SMOOTHED_EVENT_MONITOR_DIR
DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR = freshness.DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR
DEFAULT_SMOOTHED_SWITCH_READINESS_DIR = freshness.DEFAULT_SMOOTHED_SWITCH_READINESS_DIR
DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR = freshness.DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR
DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR = freshness.DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR
DEFAULT_SMOOTHED_FORWARD_WEEKLY_RUN_DIR = freshness.DEFAULT_SMOOTHED_FORWARD_WEEKLY_RUN_DIR
SYSTEM_TARGET_SAFETY = freshness.SYSTEM_TARGET_SAFETY


class DynamicV3SmoothedRefreshError(ValueError):
    """Raised when refresh/retry lineage cannot be reproduced exactly."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3SmoothedRefreshError(message)


def _generated_at(value: datetime | None) -> datetime:
    try:
        return target_core._generated_at(value)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3SmoothedRefreshError(str(exc)) from exc


def _iso_date(value: Any, *, field: str) -> date:
    try:
        return date.fromisoformat(_text(value))
    except (TypeError, ValueError) as exc:
        raise DynamicV3SmoothedRefreshError(f"{field} must be ISO date") from exc


def _texts(value: Any) -> list[str]:
    return [str(item) for item in value] if isinstance(value, list) else []


def _artifact_root(output_dir: Path, artifact_id: str | None, latest: bool, pointer: str) -> Path:
    return hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=artifact_id if not latest else None,
        pointer_name=pointer,
    )


def _write(root: Path, views: Mapping[str, bytes], pointer: str, manifest: str) -> None:
    operations._write(root, views, pointer, manifest)


def _view_errors(root: Path, views: Mapping[str, bytes]) -> list[str]:
    return operations._view_errors(root, views)


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


def _source_binding(
    *,
    kind: str,
    artifact_id: str,
    root: Path,
    validator: Callable[..., dict[str, Any]],
    validator_key: str,
    json_views: Sequence[str],
    text_views: Sequence[str] = (),
) -> dict[str, Any]:
    return promotion._source_binding(
        kind=kind,
        artifact_id=artifact_id,
        root=root,
        validator=validator,
        validator_key=validator_key,
        json_views=json_views,
        text_views=text_views,
    )


def _validate_binding(
    binding: Mapping[str, Any],
    *,
    kind: str,
    validator: Callable[..., dict[str, Any]],
    validator_key: str,
) -> list[str]:
    return promotion._validate_binding(
        binding,
        kind=kind,
        validator=validator,
        validator_key=validator_key,
    )


def _bundle_json(binding: Mapping[str, Any], name: str) -> dict[str, Any]:
    return hardening._bundle_json(binding, name)


def _source_dir(binding: Mapping[str, Any]) -> Path:
    return Path(_text(_mapping(binding.get("bundle")).get("source_dir")))


def _refresh_plan_source(refresh_plan_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="smoothed_refresh_plan",
        artifact_id=refresh_plan_id,
        root=root,
        validator=freshness.validate_smoothed_refresh_plan_artifact,
        validator_key="refresh_plan_id",
        json_views=(
            "smoothed_refresh_plan_manifest.json",
            "source_refresh_requirements.json",
            "rerun_command_plan.json",
        ),
        text_views=("smoothed_refresh_plan_report.md", "reader_brief_section.md"),
    )


def _refresh_source(refresh_execution_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="smoothed_source_refresh",
        artifact_id=refresh_execution_id,
        root=root,
        validator=validate_smoothed_source_refresh_artifact,
        validator_key="refresh_execution_id",
        json_views=(
            "smoothed_source_refresh_manifest.json",
            "refresh_execution_request.json",
            "source_refresh_results.json",
            "source_refresh_audit.json",
        ),
        text_views=("smoothed_source_refresh_report.md", "reader_brief_section.md"),
    )


def _post_source(post_refresh_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="smoothed_post_refresh_validation",
        artifact_id=post_refresh_id,
        root=root,
        validator=validate_smoothed_post_refresh_artifact,
        validator_key="post_refresh_id",
        json_views=(
            "smoothed_post_refresh_manifest.json",
            "post_refresh_data_validation.json",
            "post_refresh_preflight_result.json",
            "post_refresh_decision.json",
        ),
        text_views=("smoothed_post_refresh_report.md", "reader_brief_section.md"),
    )


def _resume_source(resume_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="smoothed_retry_resume",
        artifact_id=resume_id,
        root=root,
        validator=validate_smoothed_retry_resume_artifact,
        validator_key="resume_id",
        json_views=(
            "smoothed_retry_resume_manifest.json",
            "resume_precondition_check.json",
            "resume_steps.json",
            "resume_artifacts.json",
            "resume_summary.json",
        ),
        text_views=("smoothed_retry_resume_report.md", "reader_brief_section.md"),
    )


def _growth_source(growth_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="smoothed_sample_growth",
        artifact_id=growth_id,
        root=root,
        validator=validate_smoothed_sample_growth_artifact,
        validator_key="growth_id",
        json_views=(
            "smoothed_sample_growth_manifest.json",
            "sample_growth_summary.json",
            "sample_growth_by_target.json",
        ),
        text_views=("sample_growth_dashboard_report.md", "reader_brief_section.md"),
    )


def _bootstrap_retry_source(retry_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="smoothed_bootstrap_retry",
        artifact_id=retry_id,
        root=root,
        validator=freshness.validate_smoothed_bootstrap_retry_artifact,
        validator_key="retry_id",
        json_views=(
            "smoothed_bootstrap_retry_manifest.json",
            "retry_preflight_result.json",
            "retry_steps.json",
            "retry_artifacts.json",
            "retry_summary.json",
        ),
        text_views=("smoothed_bootstrap_retry_report.md", "reader_brief_section.md"),
    )


def _validate_frozen_source(
    source: Mapping[str, Any], *, kind: str, manifest_name: str, artifact_id_key: str
) -> list[str]:
    errors = target_core._validate_operations_source_bundle(_mapping(source.get("bundle")))
    try:
        _require(source.get("kind") == kind, f"{kind} source kind mismatch")
        artifact_id = _text(source.get("artifact_id"))
        _require(bool(artifact_id), f"{kind} source artifact id missing")
        manifest = _bundle_json(source, manifest_name)
        _require(
            manifest.get(artifact_id_key) == artifact_id,
            f"{kind} source artifact id mismatch",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _refresh_plan_lineage(source: Mapping[str, Any]) -> tuple[str, str, str, str]:
    plan_input_path = _source_dir(source) / "smoothed_refresh_plan_input_snapshot.json"
    snapshot = _read_json(plan_input_path)
    preflight_source = _mapping(snapshot.get("preflight_source"))
    preflight_input_path = (
        _source_dir(preflight_source) / "smoothed_data_preflight_input_snapshot.json"
    )
    preflight_snapshot = _read_json(preflight_input_path)
    preflight_id = _text(preflight_source.get("artifact_id"))
    model_target_dir = _text(preflight_snapshot.get("model_target_dir"))
    _require(bool(preflight_id), "refresh plan source preflight lineage missing")
    _require(bool(model_target_dir), "refresh plan model target lineage missing")
    plan_sha = _text(legacy._file_sha256(plan_input_path))
    preflight_sha = _text(legacy._file_sha256(preflight_input_path))
    _require(bool(plan_sha) and bool(preflight_sha), "refresh plan lineage checksum missing")
    return preflight_id, model_target_dir, plan_sha, preflight_sha


def _source_refresh_input(
    *,
    plan_source: Mapping[str, Any],
    config_source: Mapping[str, Any],
    execute_refresh: bool,
    source_specs: Sequence[Mapping[str, Any]],
    before_states: Mapping[str, Mapping[str, Any]],
    after_states: Mapping[str, Mapping[str, Any]],
    refresh_error: str | None,
    generated: datetime,
) -> dict[str, Any]:
    requirements = _bundle_json(plan_source, "source_refresh_requirements.json")
    requested = _iso_date(
        requirements.get("requested_as_of"), field="source refresh requested_as_of"
    )
    preflight_id, model_target_dir, plan_sha, preflight_sha = _refresh_plan_lineage(plan_source)
    return {
        "schema_version": SOURCE_REFRESH_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "requested_as_of": requested.isoformat(),
        "execute_refresh": execute_refresh,
        "refresh_plan_source": dict(plan_source),
        "config_source": dict(config_source),
        "source_preflight_id": preflight_id,
        "model_target_dir": model_target_dir,
        "refresh_plan_input_sha256": plan_sha,
        "source_preflight_input_sha256": preflight_sha,
        "source_specs": [dict(row) for row in source_specs],
        "before_states": {key: dict(value) for key, value in before_states.items()},
        "after_states": {key: dict(value) for key, value in after_states.items()},
        "refresh_error": refresh_error,
        **SYSTEM_TARGET_SAFETY,
    }


def _source_refresh_views(
    snapshot: Mapping[str, Any], *, refresh_execution_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    requested = _iso_date(snapshot.get("requested_as_of"), field="requested_as_of")
    specs = _records(snapshot.get("source_specs"))
    before = _mapping(snapshot.get("before_states"))
    after = _mapping(snapshot.get("after_states"))
    execute_refresh = snapshot.get("execute_refresh") is True
    refresh_error = _text(snapshot.get("refresh_error")) or None
    results = [
        legacy._smoothed_source_refresh_result_row(
            spec=spec,
            before=_mapping(before.get(_text(spec.get("source")))),
            after=_mapping(after.get(_text(spec.get("source")))),
            requested_as_of=requested,
            execute_refresh=execute_refresh,
            refresh_error=refresh_error,
        )
        for spec in specs
    ]
    status = legacy._smoothed_source_refresh_status(results, execute_refresh)
    plan_source = _mapping(snapshot.get("refresh_plan_source"))
    config_source = _mapping(snapshot.get("config_source"))
    config_bundle = _mapping(config_source.get("bundle"))
    config_names = list(_mapping(config_bundle.get("files")))
    config_path = str(_source_dir(config_source) / config_names[0]) if config_names else ""
    request = {
        "schema_version": 2,
        "refresh_execution_id": refresh_execution_id,
        "source_refresh_plan_id": plan_source.get("artifact_id"),
        "source_preflight_id": snapshot.get("source_preflight_id"),
        "requested_as_of": requested.isoformat(),
        "execute_refresh": execute_refresh,
        "sources_requested": [_text(spec.get("source")) for spec in specs],
        "dry_run": not execute_refresh,
        "config_path": config_path,
        "model_target_dir": snapshot.get("model_target_dir"),
        **SYSTEM_TARGET_SAFETY,
    }
    result_payload = {
        "schema_version": 2,
        "refresh_execution_id": refresh_execution_id,
        "sources": results,
        "all_sources_refreshed": status == "COMPLETED",
        "partial_refresh": status == "PARTIAL",
        "refresh_status": status,
        "external_refresh_executed": execute_refresh,
        "refresh_error": refresh_error,
        **SYSTEM_TARGET_SAFETY,
    }
    audit = legacy._smoothed_source_refresh_audit(
        refresh_execution_id=refresh_execution_id,
        before_states={key: _mapping(value) for key, value in before.items()},
        after_states={key: _mapping(value) for key, value in after.items()},
        source_results=results,
        external_refresh_executed=execute_refresh,
    )
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_source_refresh_manifest",
        "refresh_execution_id": refresh_execution_id,
        "source_refresh_plan_id": plan_source.get("artifact_id"),
        "requested_as_of": requested.isoformat(),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "refresh_status": status,
        "execute_refresh": execute_refresh,
        "smoothed_source_refresh_input_snapshot_path": str(
            root / "smoothed_source_refresh_input_snapshot.json"
        ),
        "smoothed_source_refresh_manifest_path": str(
            root / "smoothed_source_refresh_manifest.json"
        ),
        "refresh_execution_request_path": str(root / "refresh_execution_request.json"),
        "source_refresh_results_path": str(root / "source_refresh_results.json"),
        "source_refresh_audit_path": str(root / "source_refresh_audit.json"),
        "smoothed_source_refresh_report_path": str(root / "smoothed_source_refresh_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = legacy.render_smoothed_source_refresh_report(manifest, request, result_payload)
    reader = legacy.render_smoothed_source_refresh_reader_brief(result_payload)
    views = {
        "smoothed_source_refresh_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_source_refresh_manifest.json": _json_bytes(manifest),
        "refresh_execution_request.json": _json_bytes(request),
        "source_refresh_results.json": _json_bytes(result_payload),
        "source_refresh_audit.json": _json_bytes(audit),
        "smoothed_source_refresh_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "refresh_execution_request": request,
        "source_refresh_results": result_payload,
        "source_refresh_audit": audit,
        "reader_brief_section": reader,
    }


def _validate_source_refresh_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SOURCE_REFRESH_SNAPSHOT_SCHEMA,
            "source refresh snapshot schema invalid",
        )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="source refresh generated_at"
        )
        plan_source = _mapping(snapshot.get("refresh_plan_source"))
        errors.extend(
            _validate_frozen_source(
                plan_source,
                kind="smoothed_refresh_plan",
                manifest_name="smoothed_refresh_plan_manifest.json",
                artifact_id_key="refresh_plan_id",
            )
        )
        config_source = _mapping(snapshot.get("config_source"))
        errors.extend(target_core._validate_config_binding(config_source))
        freshness._source_before_consumer(
            generated, plan_source, "smoothed_refresh_plan_manifest.json"
        )
        requirements = _bundle_json(plan_source, "source_refresh_requirements.json")
        requested = _iso_date(requirements.get("requested_as_of"), field="requested_as_of")
        _require(
            snapshot.get("requested_as_of") == requested.isoformat(),
            "source refresh/plan requested date mismatch",
        )
        preflight_id, model_target_dir, plan_sha, preflight_sha = _refresh_plan_lineage(
            plan_source
        )
        _require(
            snapshot.get("source_preflight_id") == preflight_id,
            "source refresh preflight lineage mismatch",
        )
        _require(
            Path(_text(snapshot.get("model_target_dir"))).resolve()
            == Path(model_target_dir).resolve(),
            "source refresh model target lineage mismatch",
        )
        _require(
            snapshot.get("refresh_plan_input_sha256") == plan_sha
            and snapshot.get("source_preflight_input_sha256") == preflight_sha,
            "source refresh plan input commitment drift",
        )
        specs = _records(snapshot.get("source_specs"))
        expected_specs = legacy._smoothed_refresh_source_specs(
            source_rows=_records(requirements.get("source_requirements")),
            price_cache_path=Path(
                _text(
                    next(
                        row.get("cache_path")
                        for row in specs
                        if row.get("source") == "prices_daily"
                    )
                )
            ),
            marketstack_cache_path=Path(
                _text(
                    next(
                        row.get("cache_path")
                        for row in specs
                        if row.get("source") == "prices_marketstack_daily"
                    )
                )
            ),
            rates_path=Path(
                _text(
                    next(
                        row.get("cache_path")
                        for row in specs
                        if row.get("source") == "rates_daily"
                    )
                )
            ),
        )
        _require(specs == expected_specs, "source refresh specs drift")
        before = _mapping(snapshot.get("before_states"))
        after = _mapping(snapshot.get("after_states"))
        _require(set(before) == set(after), "source refresh before/after inventory mismatch")
        _require(
            set(after) == {_text(row.get("source")) for row in specs},
            "source refresh state/spec inventory mismatch",
        )
        if snapshot.get("execute_refresh") is False:
            _require(before == after, "dry-run mutated cache commitment")
            _require(not _text(snapshot.get("refresh_error")), "dry-run recorded refresh error")
        else:
            _require(snapshot.get("execute_refresh") is True, "execute_refresh must be boolean")
        current = {
            _text(spec.get("source")): legacy._cache_file_audit_state(
                _text(spec.get("source")), Path(_text(spec.get("cache_path")))
            )
            for spec in specs
        }
        _require(current == after, "source refresh live cache drift")
        _require(
            snapshot.get("production_effect") == "none",
            "source refresh production boundary invalid",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@promotion._with_validation_session
def run_smoothed_source_refresh(
    *,
    refresh_plan_id: str,
    execute_refresh: bool = False,
    refresh_plan_dir: Path = DEFAULT_SMOOTHED_REFRESH_PLAN_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_SOURCE_REFRESH_DIR,
    config_path: Path = DEFAULT_SMOOTHED_SOURCE_REFRESH_CONFIG_PATH,
    price_cache_path: Path = DEFAULT_PRICE_CACHE_PATH,
    marketstack_cache_path: Path | None = None,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
    refresh_executor: Callable[[Mapping[str, Any]], None] | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    _require(isinstance(execute_refresh, bool), "execute_refresh must be boolean")
    plan_source = _refresh_plan_source(refresh_plan_id, refresh_plan_dir)
    plan_errors = _validate_binding(
        plan_source,
        kind="smoothed_refresh_plan",
        validator=freshness.validate_smoothed_refresh_plan_artifact,
        validator_key="refresh_plan_id",
    )
    _require(not plan_errors, "; ".join(plan_errors))
    config_source = target_core._config_binding(config_path, kind="smoothed_source_refresh_policy")
    config_errors = target_core._validate_config_binding(config_source)
    _require(not config_errors, "; ".join(config_errors))
    config = legacy._load_smoothed_source_refresh_config(config_path)
    requirements = _bundle_json(plan_source, "source_refresh_requirements.json")
    requested = _iso_date(requirements.get("requested_as_of"), field="requested_as_of")
    resolved_marketstack = marketstack_cache_path or legacy._smoothed_marketstack_prices_path(
        price_cache_path
    )
    specs = legacy._smoothed_refresh_source_specs(
        source_rows=_records(requirements.get("source_requirements")),
        price_cache_path=price_cache_path,
        marketstack_cache_path=resolved_marketstack,
        rates_path=rates_path,
    )
    before = {
        _text(spec.get("source")): legacy._cache_file_audit_state(
            _text(spec.get("source")), Path(_text(spec.get("cache_path")))
        )
        for spec in specs
    }
    refresh_error: str | None = None
    if execute_refresh:
        context = {
            "refresh_plan_id": refresh_plan_id,
            "requested_as_of": requested.isoformat(),
            "source_specs": specs,
            "config": config,
            "price_cache_path": str(price_cache_path),
            "marketstack_cache_path": str(resolved_marketstack),
            "rates_path": str(rates_path),
        }
        try:
            if refresh_executor is not None:
                refresh_executor(context)
            else:
                legacy._execute_smoothed_project_data_refresh(
                    requested_as_of=requested,
                    config=config,
                    output_dir=price_cache_path.parent,
                )
        except Exception as exc:  # pragma: no cover - provider failures are environment-bound.
            refresh_error = _text(exc)
    after = {
        _text(spec.get("source")): legacy._cache_file_audit_state(
            _text(spec.get("source")), Path(_text(spec.get("cache_path")))
        )
        for spec in specs
    }
    snapshot = _source_refresh_input(
        plan_source=plan_source,
        config_source=config_source,
        execute_refresh=execute_refresh,
        source_specs=specs,
        before_states=before,
        after_states=after,
        refresh_error=refresh_error,
        generated=generated,
    )
    errors = _validate_source_refresh_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-source-refresh", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _source_refresh_views(snapshot, refresh_execution_id=root.name, root=root)
    _write(root, views, "latest_smoothed_source_refresh", "smoothed_source_refresh_manifest.json")
    return {"refresh_execution_id": root.name, "refresh_execution_dir": root, **payload}


def smoothed_source_refresh_report_payload(
    *,
    refresh_execution_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_SOURCE_REFRESH_DIR,
) -> dict[str, Any]:
    root = _artifact_root(
        output_dir, refresh_execution_id, latest, "latest_smoothed_source_refresh"
    )
    return {
        **_read_json(root / "smoothed_source_refresh_manifest.json"),
        "refresh_execution_request": _read_json(root / "refresh_execution_request.json"),
        "source_refresh_results": _read_json(root / "source_refresh_results.json"),
        "source_refresh_audit": _read_json(root / "source_refresh_audit.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "smoothed_source_refresh_input_snapshot.json"),
        "refresh_execution_dir": str(root),
    }


@promotion._with_validation_session
def validate_smoothed_source_refresh_artifact(
    *,
    refresh_execution_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_SOURCE_REFRESH_DIR,
) -> dict[str, Any]:
    root = output_dir / refresh_execution_id
    snapshot = (
        legacy._read_optional_json(root / "smoothed_source_refresh_input_snapshot.json") or {}
    )
    errors = _validate_source_refresh_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _source_refresh_views(
            snapshot, refresh_execution_id=refresh_execution_id, root=root
        )
        mismatches = _view_errors(root, views)
        _require(
            _safe(
                _mapping(payload.get("manifest")),
                _mapping(payload.get("refresh_execution_request")),
                _mapping(payload.get("source_refresh_results")),
                _mapping(payload.get("source_refresh_audit")),
            ),
            "source refresh safety fields invalid",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_source_refresh_validation",
        refresh_execution_id,
        errors,
        mismatches,
        artifact_id_key="refresh_execution_id",
    )


def _post_refresh_input(
    *,
    refresh_source: Mapping[str, Any],
    preflight_source: Mapping[str, Any],
    requested: date,
    generated: datetime,
) -> dict[str, Any]:
    return {
        "schema_version": POST_REFRESH_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "requested_as_of": requested.isoformat(),
        "refresh_source": dict(refresh_source),
        "preflight_source": dict(preflight_source),
        **SYSTEM_TARGET_SAFETY,
    }


def _post_refresh_views(
    snapshot: Mapping[str, Any], *, post_refresh_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    refresh_source = _mapping(snapshot.get("refresh_source"))
    preflight_source = _mapping(snapshot.get("preflight_source"))
    refresh_results = _bundle_json(refresh_source, "source_refresh_results.json")
    freshness_payload = _bundle_json(preflight_source, "data_freshness_snapshot.json")
    requested = _iso_date(snapshot.get("requested_as_of"), field="requested_as_of")
    data_validation = {
        "schema_version": 2,
        "post_refresh_id": post_refresh_id,
        "refresh_execution_id": refresh_source.get("artifact_id"),
        "requested_as_of": requested.isoformat(),
        "validate_data_status": freshness_payload.get("validate_data_status"),
        "errors": _texts(freshness_payload.get("blocking_errors")),
        "warnings": _texts(freshness_payload.get("warnings")),
        "latest_available": _mapping(freshness_payload.get("latest_available")),
        "source_refresh_status": refresh_results.get("refresh_status"),
        **SYSTEM_TARGET_SAFETY,
    }
    preflight_result = {
        "schema_version": 2,
        "post_refresh_id": post_refresh_id,
        "source_preflight_id": preflight_source.get("artifact_id"),
        "requested_as_of": requested.isoformat(),
        "freshness_status": freshness_payload.get("freshness_status"),
        "latest_valid_as_of": freshness_payload.get("latest_valid_as_of"),
        "blocking_errors": _texts(freshness_payload.get("blocking_errors")),
        "can_run_full_retry": freshness_payload.get("freshness_status")
        in {"READY", "READY_WITH_WARNINGS"},
        "can_run_latest_available_emission_only": (
            freshness_payload.get("freshness_status") == "LATEST_AVAILABLE_ONLY"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    decision = legacy._smoothed_post_refresh_decision(
        post_refresh_id=post_refresh_id,
        requested_as_of=requested,
        data_validation=data_validation,
        preflight_result=preflight_result,
    )
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_post_refresh_validation_manifest",
        "post_refresh_id": post_refresh_id,
        "refresh_execution_id": refresh_source.get("artifact_id"),
        "source_refresh_id": refresh_source.get("artifact_id"),
        "source_preflight_id": preflight_source.get("artifact_id"),
        "requested_as_of": requested.isoformat(),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "validate_data_status": data_validation.get("validate_data_status"),
        "freshness_status": preflight_result.get("freshness_status"),
        "retry_decision": decision.get("retry_decision"),
        "smoothed_post_refresh_validation_input_snapshot_path": str(
            root / "smoothed_post_refresh_validation_input_snapshot.json"
        ),
        "smoothed_post_refresh_manifest_path": str(root / "smoothed_post_refresh_manifest.json"),
        "post_refresh_data_validation_path": str(root / "post_refresh_data_validation.json"),
        "post_refresh_preflight_result_path": str(root / "post_refresh_preflight_result.json"),
        "post_refresh_decision_path": str(root / "post_refresh_decision.json"),
        "smoothed_post_refresh_report_path": str(root / "smoothed_post_refresh_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = legacy.render_smoothed_post_refresh_report(
        manifest, data_validation, preflight_result, decision
    )
    reader = legacy.render_smoothed_post_refresh_reader_brief(decision, preflight_result)
    views = {
        "smoothed_post_refresh_validation_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_post_refresh_manifest.json": _json_bytes(manifest),
        "post_refresh_data_validation.json": _json_bytes(data_validation),
        "post_refresh_preflight_result.json": _json_bytes(preflight_result),
        "post_refresh_decision.json": _json_bytes(decision),
        "smoothed_post_refresh_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "post_refresh_data_validation": data_validation,
        "post_refresh_preflight_result": preflight_result,
        "post_refresh_decision": decision,
        "reader_brief_section": reader,
    }


def _validate_post_refresh_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == POST_REFRESH_SNAPSHOT_SCHEMA,
            "post-refresh snapshot schema invalid",
        )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="post-refresh generated_at"
        )
        refresh_source = _mapping(snapshot.get("refresh_source"))
        preflight_source = _mapping(snapshot.get("preflight_source"))
        errors.extend(
            _validate_binding(
                refresh_source,
                kind="smoothed_source_refresh",
                validator=validate_smoothed_source_refresh_artifact,
                validator_key="refresh_execution_id",
            )
        )
        errors.extend(freshness._validate_preflight_source(preflight_source))
        freshness._source_before_consumer(
            generated, refresh_source, "smoothed_source_refresh_manifest.json"
        )
        freshness._source_before_consumer(
            generated, preflight_source, "smoothed_data_preflight_manifest.json"
        )
        refresh_request = _bundle_json(refresh_source, "refresh_execution_request.json")
        refresh_results = _bundle_json(refresh_source, "source_refresh_results.json")
        preflight = _bundle_json(preflight_source, "data_freshness_snapshot.json")
        preflight_snapshot = _read_json(
            _source_dir(preflight_source) / "smoothed_data_preflight_input_snapshot.json"
        )
        requested = _text(snapshot.get("requested_as_of"))
        _require(
            requested == refresh_request.get("requested_as_of")
            == refresh_results.get("sources", [{}])[0].get("required_through")
            == preflight.get("requested_date"),
            "post-refresh requested date lineage mismatch",
        )
        _require(
            Path(_text(preflight_snapshot.get("model_target_dir"))).resolve()
            == Path(_text(refresh_request.get("model_target_dir"))).resolve(),
            "post-refresh model target lineage mismatch",
        )
        specs = {
            _text(row.get("source")): Path(_text(row.get("cache_path"))).resolve()
            for row in _records(
                _read_json(
                    _source_dir(refresh_source) / "smoothed_source_refresh_input_snapshot.json"
                ).get("source_specs")
            )
        }
        _require(
            Path(_text(preflight_snapshot.get("price_cache_path"))).resolve()
            == specs.get("prices_daily"),
            "post-refresh price cache lineage mismatch",
        )
        _require(
            Path(_text(preflight_snapshot.get("rates_path"))).resolve()
            == specs.get("rates_daily"),
            "post-refresh rates cache lineage mismatch",
        )
        _require(
            snapshot.get("production_effect") == "none",
            "post-refresh production boundary invalid",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@promotion._with_validation_session
def run_smoothed_post_refresh_validation(
    *,
    refresh_execution_id: str,
    requested_as_of: date | None = None,
    refresh_execution_dir: Path = DEFAULT_SMOOTHED_SOURCE_REFRESH_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_POST_REFRESH_VALIDATION_DIR,
    preflight_dir: Path = DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
    price_cache_path: Path = DEFAULT_PRICE_CACHE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    model_target_dir: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    refresh_source = _refresh_source(refresh_execution_id, refresh_execution_dir)
    source_errors = _validate_binding(
        refresh_source,
        kind="smoothed_source_refresh",
        validator=validate_smoothed_source_refresh_artifact,
        validator_key="refresh_execution_id",
    )
    _require(not source_errors, "; ".join(source_errors))
    request = _bundle_json(refresh_source, "refresh_execution_request.json")
    requested = _iso_date(request.get("requested_as_of"), field="refresh requested_as_of")
    _require(
        requested_as_of is None or requested_as_of == requested,
        "post-refresh requested date differs from source refresh",
    )
    lineage_model_target_dir = Path(_text(request.get("model_target_dir")))
    _require(
        model_target_dir is None
        or model_target_dir.resolve() == lineage_model_target_dir.resolve(),
        "post-refresh model target lineage mismatch",
    )
    refresh_snapshot = _read_json(
        refresh_execution_dir
        / refresh_execution_id
        / "smoothed_source_refresh_input_snapshot.json"
    )
    spec_paths = {
        _text(row.get("source")): Path(_text(row.get("cache_path"))).resolve()
        for row in _records(refresh_snapshot.get("source_specs"))
    }
    _require(
        price_cache_path.resolve() == spec_paths.get("prices_daily"),
        "post-refresh price cache differs from source refresh",
    )
    _require(
        rates_path.resolve() == spec_paths.get("rates_daily"),
        "post-refresh rates cache differs from source refresh",
    )
    preflight = freshness.run_smoothed_data_preflight(
        requested_as_of=requested,
        output_dir=preflight_dir,
        price_cache_path=price_cache_path,
        rates_path=rates_path,
        model_target_dir=lineage_model_target_dir,
        generated_at=generated,
    )
    preflight_source = freshness._preflight_source(preflight["preflight_id"], preflight_dir)
    preflight_errors = freshness._validate_preflight_source(preflight_source)
    _require(not preflight_errors, "; ".join(preflight_errors))
    snapshot = _post_refresh_input(
        refresh_source=refresh_source,
        preflight_source=preflight_source,
        requested=requested,
        generated=generated,
    )
    errors = _validate_post_refresh_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-post-refresh", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _post_refresh_views(snapshot, post_refresh_id=root.name, root=root)
    _write(
        root,
        views,
        "latest_smoothed_post_refresh_validation",
        "smoothed_post_refresh_manifest.json",
    )
    return {
        "post_refresh_id": root.name,
        "post_refresh_dir": root,
        "preflight": preflight,
        **payload,
    }


def smoothed_post_refresh_validation_report_payload(
    *,
    post_refresh_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_POST_REFRESH_VALIDATION_DIR,
) -> dict[str, Any]:
    root = _artifact_root(
        output_dir, post_refresh_id, latest, "latest_smoothed_post_refresh_validation"
    )
    return {
        **_read_json(root / "smoothed_post_refresh_manifest.json"),
        "post_refresh_data_validation": _read_json(root / "post_refresh_data_validation.json"),
        "post_refresh_preflight_result": _read_json(root / "post_refresh_preflight_result.json"),
        "post_refresh_decision": _read_json(root / "post_refresh_decision.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(
            root / "smoothed_post_refresh_validation_input_snapshot.json"
        ),
        "post_refresh_dir": str(root),
    }


@promotion._with_validation_session
def validate_smoothed_post_refresh_artifact(
    *,
    post_refresh_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_POST_REFRESH_VALIDATION_DIR,
) -> dict[str, Any]:
    root = output_dir / post_refresh_id
    snapshot = (
        legacy._read_optional_json(root / "smoothed_post_refresh_validation_input_snapshot.json")
        or {}
    )
    errors = _validate_post_refresh_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _post_refresh_views(snapshot, post_refresh_id=post_refresh_id, root=root)
        mismatches = _view_errors(root, views)
        _require(
            _safe(
                _mapping(payload.get("manifest")),
                _mapping(payload.get("post_refresh_data_validation")),
                _mapping(payload.get("post_refresh_preflight_result")),
                _mapping(payload.get("post_refresh_decision")),
            ),
            "post-refresh safety fields invalid",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_post_refresh_validation",
        post_refresh_id,
        errors,
        mismatches,
        artifact_id_key="post_refresh_id",
    )


def _run_bootstrap_retry_from_preflight(
    *,
    preflight_source: Mapping[str, Any],
    requested: date,
    generated: datetime,
    bootstrap_retry_dir: Path,
    preflight_dir: Path,
    latest_emission_dir: Path,
    model_target_dir: Path,
    emission_dir: Path,
    due_dir: Path,
    update_dir: Path,
    classification_dir: Path,
    binding_dir: Path,
    progress_dir: Path,
    dashboard_dir: Path,
    monitor_dir: Path,
    switch_plan_dir: Path,
    recheck_dir: Path,
    owner_promotion_dir: Path,
    renewal_dir: Path,
    weekly_run_dir: Path,
    binding_id: str | None,
    switch_plan_id: str | None,
    owner_promotion_id: str | None,
    price_cache_path: Path,
    rates_path: Path,
) -> dict[str, Any]:
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
    child_type, child_source = freshness._retry_child(
        status=status,
        requested=requested,
        preflight_id=_text(preflight_source.get("artifact_id")),
        generated=generated,
        paths=paths,
        binding_id=binding_id,
        switch_plan_id=switch_plan_id,
        owner_promotion_id=owner_promotion_id,
    )
    snapshot = {
        "schema_version": freshness.RETRY_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "requested_as_of": requested.isoformat(),
        "requested_week_ending": None,
        "requested_date": requested.isoformat(),
        "binding_id": binding_id,
        "switch_plan_id": switch_plan_id,
        "owner_promotion_id": owner_promotion_id,
        "preflight_source": dict(preflight_source),
        "child_type": child_type,
        "child_source": child_source,
        **SYSTEM_TARGET_SAFETY,
    }
    errors = freshness._validate_retry_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-bootstrap-retry", snapshot)
    root = _unique_dir(bootstrap_retry_dir / artifact_id)
    views, payload = freshness._retry_views(snapshot, retry_id=root.name, root=root)
    freshness._write(
        root,
        views,
        "latest_smoothed_bootstrap_retry",
        "smoothed_bootstrap_retry_manifest.json",
    )
    return {"retry_id": root.name, "retry_dir": root, **payload}


def _retry_resume_views(
    snapshot: Mapping[str, Any], *, resume_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    post_source = _mapping(snapshot.get("post_refresh_source"))
    decision = _bundle_json(post_source, "post_refresh_decision.json")
    data_validation = _bundle_json(post_source, "post_refresh_data_validation.json")
    preflight_result = _bundle_json(post_source, "post_refresh_preflight_result.json")
    requested = _iso_date(snapshot.get("requested_as_of"), field="requested_as_of")
    can_resume = (
        decision.get("retry_decision") == "RETRY_READY"
        and data_validation.get("validate_data_status") in {"PASS", "PASS_WITH_WARNINGS"}
        and preflight_result.get("can_run_full_retry") is True
    )
    child_source = _mapping(snapshot.get("bootstrap_retry_source"))
    retry: dict[str, Any] | None = None
    if child_source:
        retry = {
            "retry_id": child_source.get("artifact_id"),
            "retry_summary": _bundle_json(child_source, "retry_summary.json"),
            "retry_artifacts": _bundle_json(child_source, "retry_artifacts.json"),
        }
    before = _mapping(snapshot.get("before_counts"))
    precondition = {
        "schema_version": 2,
        "resume_id": resume_id,
        "post_refresh_id": post_source.get("artifact_id"),
        "retry_decision": decision.get("retry_decision"),
        "precondition_status": "PASS" if can_resume else "BLOCKED",
        "can_resume": can_resume,
        "blocking_errors": _texts(preflight_result.get("blocking_errors")),
        "required_sources_fresh": preflight_result.get("can_run_full_retry") is True,
        "validate_data_status": data_validation.get("validate_data_status"),
        "available_forward_events_before_resume": before.get("forward", 0),
        "available_sideways_events_before_resume": before.get("sideways", 0),
        "available_recovery_events_before_resume": before.get("recovery", 0),
        **SYSTEM_TARGET_SAFETY,
    }
    steps = legacy._smoothed_retry_resume_steps(can_resume=can_resume, retry=retry)
    artifacts = legacy._smoothed_retry_resume_artifacts(retry)
    summary = legacy._smoothed_retry_resume_summary(
        resume_id=resume_id,
        requested_as_of=requested,
        can_resume=can_resume,
        retry=retry,
    )
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_retry_resume_manifest",
        "resume_id": resume_id,
        "post_refresh_id": post_source.get("artifact_id"),
        "bootstrap_retry_id": child_source.get("artifact_id"),
        "requested_as_of": requested.isoformat(),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "resume_status": summary.get("resume_status"),
        "smoothed_retry_resume_input_snapshot_path": str(
            root / "smoothed_retry_resume_input_snapshot.json"
        ),
        "smoothed_retry_resume_manifest_path": str(root / "smoothed_retry_resume_manifest.json"),
        "resume_precondition_check_path": str(root / "resume_precondition_check.json"),
        "resume_steps_path": str(root / "resume_steps.json"),
        "resume_artifacts_path": str(root / "resume_artifacts.json"),
        "resume_summary_path": str(root / "resume_summary.json"),
        "smoothed_retry_resume_report_path": str(root / "smoothed_retry_resume_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = legacy.render_smoothed_retry_resume_report(manifest, precondition, steps, summary)
    reader = legacy.render_smoothed_retry_resume_reader_brief(summary)
    views = {
        "smoothed_retry_resume_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_retry_resume_manifest.json": _json_bytes(manifest),
        "resume_precondition_check.json": _json_bytes(precondition),
        "resume_steps.json": _json_bytes(steps),
        "resume_artifacts.json": _json_bytes(artifacts),
        "resume_summary.json": _json_bytes(summary),
        "smoothed_retry_resume_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "resume_precondition_check": precondition,
        "resume_steps": steps,
        "resume_artifacts": artifacts,
        "resume_summary": summary,
        "reader_brief_section": reader,
    }


def _validate_retry_resume_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == RETRY_RESUME_SNAPSHOT_SCHEMA,
            "retry resume snapshot schema invalid",
        )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="retry resume generated_at"
        )
        post_source = _mapping(snapshot.get("post_refresh_source"))
        errors.extend(
            _validate_binding(
                post_source,
                kind="smoothed_post_refresh_validation",
                validator=validate_smoothed_post_refresh_artifact,
                validator_key="post_refresh_id",
            )
        )
        freshness._source_before_consumer(
            generated, post_source, "smoothed_post_refresh_manifest.json"
        )
        decision = _bundle_json(post_source, "post_refresh_decision.json")
        data_validation = _bundle_json(post_source, "post_refresh_data_validation.json")
        preflight_result = _bundle_json(post_source, "post_refresh_preflight_result.json")
        can_resume = (
            decision.get("retry_decision") == "RETRY_READY"
            and data_validation.get("validate_data_status") in {"PASS", "PASS_WITH_WARNINGS"}
            and preflight_result.get("can_run_full_retry") is True
        )
        _require(
            snapshot.get("requested_as_of") == preflight_result.get("requested_as_of"),
            "retry resume requested date lineage mismatch",
        )
        before = _mapping(snapshot.get("before_counts"))
        _require(
            set(before) == {"forward", "sideways", "recovery"}
            and all(
                isinstance(value, int) and not isinstance(value, bool) and value >= 0
                for value in before.values()
            ),
            "retry resume before counts invalid",
        )
        child_source = _mapping(snapshot.get("bootstrap_retry_source"))
        if can_resume:
            _require(bool(child_source), "retry-ready resume child missing")
            errors.extend(
                _validate_binding(
                    child_source,
                    kind="smoothed_bootstrap_retry",
                    validator=freshness.validate_smoothed_bootstrap_retry_artifact,
                    validator_key="retry_id",
                )
            )
            freshness._source_before_consumer(
                generated, child_source, "smoothed_bootstrap_retry_manifest.json"
            )
            child_manifest = _bundle_json(child_source, "smoothed_bootstrap_retry_manifest.json")
            _require(
                child_manifest.get("requested_as_of") == snapshot.get("requested_as_of"),
                "retry resume child requested date mismatch",
            )
            child_input = _read_json(
                _source_dir(child_source) / "smoothed_bootstrap_retry_input_snapshot.json"
            )
            post_input = _read_json(
                _source_dir(post_source) / "smoothed_post_refresh_validation_input_snapshot.json"
            )
            _require(
                _mapping(child_input.get("preflight_source")).get("artifact_id")
                == _mapping(post_input.get("preflight_source")).get("artifact_id"),
                "retry resume child/preflight lineage mismatch",
            )
        else:
            _require(not child_source, "blocked retry resume must not bind a child artifact")
        _require(
            snapshot.get("production_effect") == "none",
            "retry resume production boundary invalid",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@promotion._with_validation_session
def run_smoothed_retry_resume(
    *,
    post_refresh_id: str,
    post_refresh_dir: Path = DEFAULT_SMOOTHED_POST_REFRESH_VALIDATION_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_RETRY_RESUME_DIR,
    bootstrap_retry_dir: Path = DEFAULT_SMOOTHED_BOOTSTRAP_RETRY_DIR,
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
    post_source = _post_source(post_refresh_id, post_refresh_dir)
    source_errors = _validate_binding(
        post_source,
        kind="smoothed_post_refresh_validation",
        validator=validate_smoothed_post_refresh_artifact,
        validator_key="post_refresh_id",
    )
    _require(not source_errors, "; ".join(source_errors))
    decision = _bundle_json(post_source, "post_refresh_decision.json")
    validation = _bundle_json(post_source, "post_refresh_data_validation.json")
    preflight_result = _bundle_json(post_source, "post_refresh_preflight_result.json")
    requested = _iso_date(preflight_result.get("requested_as_of"), field="requested_as_of")
    can_resume = (
        decision.get("retry_decision") == "RETRY_READY"
        and validation.get("validate_data_status") in {"PASS", "PASS_WITH_WARNINGS"}
        and preflight_result.get("can_run_full_retry") is True
    )
    before_counts = legacy._latest_smoothed_progress_counts(progress_dir)
    retry: dict[str, Any] | None = None
    child_source: dict[str, Any] | None = None
    if can_resume:
        post_input = _read_json(
            post_refresh_dir
            / post_refresh_id
            / "smoothed_post_refresh_validation_input_snapshot.json"
        )
        preflight_source = _mapping(post_input.get("preflight_source"))
        preflight_input = _read_json(
            _source_dir(preflight_source) / "smoothed_data_preflight_input_snapshot.json"
        )
        _require(
            model_target_dir.resolve()
            == Path(_text(preflight_input.get("model_target_dir"))).resolve(),
            "retry resume model target differs from post-refresh lineage",
        )
        _require(
            price_cache_path.resolve()
            == Path(_text(preflight_input.get("price_cache_path"))).resolve(),
            "retry resume price cache differs from post-refresh lineage",
        )
        _require(
            rates_path.resolve() == Path(_text(preflight_input.get("rates_path"))).resolve(),
            "retry resume rates cache differs from post-refresh lineage",
        )
        retry = _run_bootstrap_retry_from_preflight(
            preflight_source=preflight_source,
            requested=requested,
            generated=generated,
            bootstrap_retry_dir=bootstrap_retry_dir,
            preflight_dir=preflight_dir,
            latest_emission_dir=latest_emission_dir,
            model_target_dir=model_target_dir,
            emission_dir=emission_dir,
            due_dir=due_dir,
            update_dir=update_dir,
            classification_dir=classification_dir,
            binding_dir=binding_dir,
            progress_dir=progress_dir,
            dashboard_dir=dashboard_dir,
            monitor_dir=monitor_dir,
            switch_plan_dir=switch_plan_dir,
            recheck_dir=recheck_dir,
            owner_promotion_dir=owner_promotion_dir,
            renewal_dir=renewal_dir,
            weekly_run_dir=weekly_run_dir,
            binding_id=binding_id,
            switch_plan_id=switch_plan_id,
            owner_promotion_id=owner_promotion_id,
            price_cache_path=price_cache_path,
            rates_path=rates_path,
        )
        child_source = _bootstrap_retry_source(retry["retry_id"], bootstrap_retry_dir)
    snapshot = {
        "schema_version": RETRY_RESUME_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "requested_as_of": requested.isoformat(),
        "post_refresh_source": post_source,
        "before_counts": before_counts,
        "bootstrap_retry_source": child_source,
        **SYSTEM_TARGET_SAFETY,
    }
    errors = _validate_retry_resume_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-retry-resume", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _retry_resume_views(snapshot, resume_id=root.name, root=root)
    _write(root, views, "latest_smoothed_retry_resume", "smoothed_retry_resume_manifest.json")
    return {
        "resume_id": root.name,
        "resume_dir": root,
        "bootstrap_retry": retry,
        **payload,
    }


def smoothed_retry_resume_report_payload(
    *,
    resume_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_RETRY_RESUME_DIR,
) -> dict[str, Any]:
    root = _artifact_root(output_dir, resume_id, latest, "latest_smoothed_retry_resume")
    return {
        **_read_json(root / "smoothed_retry_resume_manifest.json"),
        "resume_precondition_check": _read_json(root / "resume_precondition_check.json"),
        "resume_steps": _read_json(root / "resume_steps.json"),
        "resume_artifacts": _read_json(root / "resume_artifacts.json"),
        "resume_summary": _read_json(root / "resume_summary.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "smoothed_retry_resume_input_snapshot.json"),
        "resume_dir": str(root),
    }


@promotion._with_validation_session
def validate_smoothed_retry_resume_artifact(
    *,
    resume_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_RETRY_RESUME_DIR,
) -> dict[str, Any]:
    root = output_dir / resume_id
    snapshot = legacy._read_optional_json(root / "smoothed_retry_resume_input_snapshot.json") or {}
    errors = _validate_retry_resume_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _retry_resume_views(snapshot, resume_id=resume_id, root=root)
        mismatches = _view_errors(root, views)
        summary = _mapping(payload.get("resume_summary"))
        _require(
            _safe(
                _mapping(payload.get("manifest")),
                _mapping(payload.get("resume_precondition_check")),
                _mapping(payload.get("resume_steps")),
                _mapping(payload.get("resume_artifacts")),
                summary,
            ),
            "retry resume safety fields invalid",
        )
        _require(summary.get("can_execute_switch") is False, "retry resume switch enabled")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_retry_resume_validation",
        resume_id,
        errors,
        mismatches,
        artifact_id_key="resume_id",
    )


def _sample_growth_views(
    snapshot: Mapping[str, Any], *, growth_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    resume_source = _mapping(snapshot.get("resume_source"))
    precondition = _bundle_json(resume_source, "resume_precondition_check.json")
    resume_summary = _bundle_json(resume_source, "resume_summary.json")
    summary = legacy._smoothed_sample_growth_summary(
        growth_id=growth_id,
        resume_id=_text(resume_source.get("artifact_id")),
        precondition=precondition,
        resume_summary=resume_summary,
    )
    by_target = legacy._smoothed_sample_growth_by_target(summary)
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_sample_growth_manifest",
        "growth_id": growth_id,
        "resume_id": resume_source.get("artifact_id"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "growth_status": summary.get("growth_status"),
        "smoothed_sample_growth_input_snapshot_path": str(
            root / "smoothed_sample_growth_input_snapshot.json"
        ),
        "smoothed_sample_growth_manifest_path": str(root / "smoothed_sample_growth_manifest.json"),
        "sample_growth_summary_path": str(root / "sample_growth_summary.json"),
        "sample_growth_by_target_path": str(root / "sample_growth_by_target.json"),
        "sample_growth_dashboard_report_path": str(root / "sample_growth_dashboard_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = legacy.render_smoothed_sample_growth_report(manifest, summary, by_target)
    reader = legacy.render_smoothed_sample_growth_reader_brief(summary)
    views = {
        "smoothed_sample_growth_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_sample_growth_manifest.json": _json_bytes(manifest),
        "sample_growth_summary.json": _json_bytes(summary),
        "sample_growth_by_target.json": _json_bytes(by_target),
        "sample_growth_dashboard_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "sample_growth_summary": summary,
        "sample_growth_by_target": by_target,
        "reader_brief_section": reader,
    }


def _validate_sample_growth_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SAMPLE_GROWTH_SNAPSHOT_SCHEMA,
            "sample growth snapshot schema invalid",
        )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="sample growth generated_at"
        )
        resume_source = _mapping(snapshot.get("resume_source"))
        errors.extend(
            _validate_binding(
                resume_source,
                kind="smoothed_retry_resume",
                validator=validate_smoothed_retry_resume_artifact,
                validator_key="resume_id",
            )
        )
        freshness._source_before_consumer(
            generated, resume_source, "smoothed_retry_resume_manifest.json"
        )
        _require(
            snapshot.get("production_effect") == "none",
            "sample growth production boundary invalid",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@promotion._with_validation_session
def build_smoothed_sample_growth(
    *,
    resume_id: str,
    resume_dir: Path = DEFAULT_SMOOTHED_RETRY_RESUME_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_SAMPLE_GROWTH_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    resume_source = _resume_source(resume_id, resume_dir)
    source_errors = _validate_binding(
        resume_source,
        kind="smoothed_retry_resume",
        validator=validate_smoothed_retry_resume_artifact,
        validator_key="resume_id",
    )
    _require(not source_errors, "; ".join(source_errors))
    snapshot = {
        "schema_version": SAMPLE_GROWTH_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "resume_source": resume_source,
        **SYSTEM_TARGET_SAFETY,
    }
    errors = _validate_sample_growth_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-sample-growth", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _sample_growth_views(snapshot, growth_id=root.name, root=root)
    _write(root, views, "latest_smoothed_sample_growth", "smoothed_sample_growth_manifest.json")
    return {"growth_id": root.name, "growth_dir": root, **payload}


def smoothed_sample_growth_report_payload(
    *,
    growth_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_SAMPLE_GROWTH_DIR,
) -> dict[str, Any]:
    root = _artifact_root(output_dir, growth_id, latest, "latest_smoothed_sample_growth")
    return {
        **_read_json(root / "smoothed_sample_growth_manifest.json"),
        "sample_growth_summary": _read_json(root / "sample_growth_summary.json"),
        "sample_growth_by_target": _read_json(root / "sample_growth_by_target.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "smoothed_sample_growth_input_snapshot.json"),
        "growth_dir": str(root),
    }


@promotion._with_validation_session
def validate_smoothed_sample_growth_artifact(
    *,
    growth_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_SAMPLE_GROWTH_DIR,
) -> dict[str, Any]:
    root = output_dir / growth_id
    snapshot = legacy._read_optional_json(root / "smoothed_sample_growth_input_snapshot.json") or {}
    errors = _validate_sample_growth_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _sample_growth_views(snapshot, growth_id=growth_id, root=root)
        mismatches = _view_errors(root, views)
        summary = _mapping(payload.get("sample_growth_summary"))
        _require(
            legacy._smoothed_sample_growth_delta_consistent(summary),
            "sample growth delta inconsistent",
        )
        _require(
            _safe(
                _mapping(payload.get("manifest")),
                summary,
                _mapping(payload.get("sample_growth_by_target")),
            ),
            "sample growth safety fields invalid",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_sample_growth_validation",
        growth_id,
        errors,
        mismatches,
        artifact_id_key="growth_id",
    )


def _data_readiness_views(
    snapshot: Mapping[str, Any], *, readiness_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    refresh_source = _mapping(snapshot.get("refresh_source"))
    post_source = _mapping(snapshot.get("post_refresh_source"))
    resume_source = _mapping(snapshot.get("resume_source"))
    growth_source = _mapping(snapshot.get("growth_source"))
    refresh = {
        **_bundle_json(refresh_source, "smoothed_source_refresh_manifest.json"),
        "source_refresh_results": _bundle_json(refresh_source, "source_refresh_results.json"),
    }
    post = {
        **_bundle_json(post_source, "smoothed_post_refresh_manifest.json"),
        "post_refresh_decision": _bundle_json(post_source, "post_refresh_decision.json"),
    }
    resume = {
        **_bundle_json(resume_source, "smoothed_retry_resume_manifest.json"),
        "resume_summary": _bundle_json(resume_source, "resume_summary.json"),
    }
    growth = {
        **_bundle_json(growth_source, "smoothed_sample_growth_manifest.json"),
        "sample_growth_summary": _bundle_json(growth_source, "sample_growth_summary.json"),
    }
    summary = legacy._smoothed_data_readiness_summary(
        readiness_id=readiness_id,
        refresh=refresh,
        post_refresh=post,
        resume=resume,
        growth=growth,
    )
    checklist = legacy.render_smoothed_data_readiness_checklist(summary)
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_data_readiness_manifest",
        "readiness_id": readiness_id,
        "refresh_execution_id": refresh_source.get("artifact_id"),
        "post_refresh_id": post_source.get("artifact_id"),
        "resume_id": resume_source.get("artifact_id"),
        "growth_id": growth_source.get("artifact_id"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "current_status": summary.get("current_status"),
        "recommended_owner_action": summary.get("recommended_owner_action"),
        "smoothed_data_readiness_input_snapshot_path": str(
            root / "smoothed_data_readiness_input_snapshot.json"
        ),
        "smoothed_data_readiness_manifest_path": str(
            root / "smoothed_data_readiness_manifest.json"
        ),
        "owner_data_readiness_summary_path": str(root / "owner_data_readiness_summary.json"),
        "owner_data_readiness_checklist_path": str(root / "owner_data_readiness_checklist.md"),
        "smoothed_data_readiness_report_path": str(root / "smoothed_data_readiness_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = legacy.render_smoothed_data_readiness_report(manifest, summary, checklist)
    reader = legacy.render_smoothed_data_readiness_reader_brief(summary)
    views = {
        "smoothed_data_readiness_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_data_readiness_manifest.json": _json_bytes(manifest),
        "owner_data_readiness_summary.json": _json_bytes(summary),
        "owner_data_readiness_checklist.md": checklist.encode("utf-8"),
        "smoothed_data_readiness_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "owner_data_readiness_summary": summary,
        "owner_data_readiness_checklist": checklist,
        "reader_brief_section": reader,
    }


def _validate_data_readiness_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == DATA_READINESS_SNAPSHOT_SCHEMA,
            "data readiness snapshot schema invalid",
        )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="data readiness generated_at"
        )
        refresh_source = _mapping(snapshot.get("refresh_source"))
        post_source = _mapping(snapshot.get("post_refresh_source"))
        resume_source = _mapping(snapshot.get("resume_source"))
        growth_source = _mapping(snapshot.get("growth_source"))
        specifications = (
            (
                refresh_source,
                "smoothed_source_refresh",
                validate_smoothed_source_refresh_artifact,
                "refresh_execution_id",
                "smoothed_source_refresh_manifest.json",
            ),
            (
                post_source,
                "smoothed_post_refresh_validation",
                validate_smoothed_post_refresh_artifact,
                "post_refresh_id",
                "smoothed_post_refresh_manifest.json",
            ),
            (
                resume_source,
                "smoothed_retry_resume",
                validate_smoothed_retry_resume_artifact,
                "resume_id",
                "smoothed_retry_resume_manifest.json",
            ),
            (
                growth_source,
                "smoothed_sample_growth",
                validate_smoothed_sample_growth_artifact,
                "growth_id",
                "smoothed_sample_growth_manifest.json",
            ),
        )
        for source, kind, validator, key, manifest_name in specifications:
            errors.extend(
                _validate_binding(
                    source,
                    kind=kind,
                    validator=validator,
                    validator_key=key,
                )
            )
            freshness._source_before_consumer(generated, source, manifest_name)
        refresh_manifest = _bundle_json(refresh_source, "smoothed_source_refresh_manifest.json")
        post_manifest = _bundle_json(post_source, "smoothed_post_refresh_manifest.json")
        resume_manifest = _bundle_json(resume_source, "smoothed_retry_resume_manifest.json")
        growth_manifest = _bundle_json(growth_source, "smoothed_sample_growth_manifest.json")
        _require(
            post_manifest.get("refresh_execution_id") == refresh_source.get("artifact_id"),
            "data readiness refresh/post lineage mismatch",
        )
        _require(
            resume_manifest.get("post_refresh_id") == post_source.get("artifact_id"),
            "data readiness post/resume lineage mismatch",
        )
        _require(
            growth_manifest.get("resume_id") == resume_source.get("artifact_id"),
            "data readiness resume/growth lineage mismatch",
        )
        requested_dates = {
            _text(refresh_manifest.get("requested_as_of")),
            _text(post_manifest.get("requested_as_of")),
            _text(resume_manifest.get("requested_as_of")),
        }
        _require(len(requested_dates) == 1 and "" not in requested_dates, "readiness date drift")
        _require(
            snapshot.get("production_effect") == "none",
            "data readiness production boundary invalid",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@promotion._with_validation_session
def pack_smoothed_data_readiness(
    *,
    refresh_execution_id: str,
    post_refresh_id: str,
    resume_id: str,
    growth_id: str,
    refresh_execution_dir: Path = DEFAULT_SMOOTHED_SOURCE_REFRESH_DIR,
    post_refresh_dir: Path = DEFAULT_SMOOTHED_POST_REFRESH_VALIDATION_DIR,
    resume_dir: Path = DEFAULT_SMOOTHED_RETRY_RESUME_DIR,
    growth_dir: Path = DEFAULT_SMOOTHED_SAMPLE_GROWTH_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_DATA_READINESS_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    refresh_source = _refresh_source(refresh_execution_id, refresh_execution_dir)
    post_source = _post_source(post_refresh_id, post_refresh_dir)
    resume_source = _resume_source(resume_id, resume_dir)
    growth_source = _growth_source(growth_id, growth_dir)
    snapshot = {
        "schema_version": DATA_READINESS_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "refresh_source": refresh_source,
        "post_refresh_source": post_source,
        "resume_source": resume_source,
        "growth_source": growth_source,
        **SYSTEM_TARGET_SAFETY,
    }
    errors = _validate_data_readiness_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-data-readiness", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _data_readiness_views(snapshot, readiness_id=root.name, root=root)
    _write(root, views, "latest_smoothed_data_readiness", "smoothed_data_readiness_manifest.json")
    return {"readiness_id": root.name, "readiness_dir": root, **payload}


def smoothed_data_readiness_report_payload(
    *,
    readiness_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_DATA_READINESS_DIR,
) -> dict[str, Any]:
    root = _artifact_root(output_dir, readiness_id, latest, "latest_smoothed_data_readiness")
    return {
        **_read_json(root / "smoothed_data_readiness_manifest.json"),
        "owner_data_readiness_summary": _read_json(root / "owner_data_readiness_summary.json"),
        "owner_data_readiness_checklist": (
            root / "owner_data_readiness_checklist.md"
        ).read_text(encoding="utf-8"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "smoothed_data_readiness_input_snapshot.json"),
        "readiness_dir": str(root),
    }


@promotion._with_validation_session
def validate_smoothed_data_readiness_artifact(
    *,
    readiness_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_DATA_READINESS_DIR,
) -> dict[str, Any]:
    root = output_dir / readiness_id
    snapshot = (
        legacy._read_optional_json(root / "smoothed_data_readiness_input_snapshot.json") or {}
    )
    errors = _validate_data_readiness_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _data_readiness_views(snapshot, readiness_id=readiness_id, root=root)
        mismatches = _view_errors(root, views)
        _require(
            _safe(
                _mapping(payload.get("manifest")),
                _mapping(payload.get("owner_data_readiness_summary")),
            ),
            "data readiness safety fields invalid",
        )
        _require(
            "no broker" in _text(payload.get("owner_data_readiness_checklist")).lower(),
            "data readiness checklist safety statement missing",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_data_readiness_validation",
        readiness_id,
        errors,
        mismatches,
        artifact_id_key="readiness_id",
    )
