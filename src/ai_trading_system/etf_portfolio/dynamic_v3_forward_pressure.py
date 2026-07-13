from __future__ import annotations

import math
import tempfile
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    DEFAULT_PRESSURE_REGIME_TAG_DIR,
    _file_bytes_match,
    _json_bytes,
    _jsonl_bytes,
    _operations_datetime,
    _operations_generated_at,
    _operations_source_bundle,
    _report_input_snapshot,
    _semantic_artifact_id,
    _validate_operations_source_bundle,
    run_pressure_regime_tagging,
    validate_pressure_regime_tag_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_defensive_evidence import (
    AI_AFTER_CHATGPT_START,
    DEFAULT_FORWARD_PRESSURE_CAPTURE_CONFIG_PATH,
    DEFAULT_FORWARD_PRESSURE_CAPTURE_DIR,
    DEFAULT_PRESSURE_CAPTURE_DIR,
    DEFAULT_PRESSURE_SAMPLE_LEDGER_DIR,
    DEFAULT_PRESSURE_TRIGGER_DIR,
    DEFAULT_WEEKLY_DEFENSIVE_EVIDENCE_DIR,
    _command_pack,
    _event_trigger_plan,
    _quality_gate_for_pressure_trigger,
    _trigger_metrics,
    _trigger_status,
    _triggered_actions,
    render_forward_pressure_capture_report,
    render_pressure_capture_report,
    render_pressure_sample_ledger_report,
    render_pressure_trigger_report,
    render_weekly_defensive_reader_brief,
    render_weekly_defensive_report,
)
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DEFAULT_RATES_CACHE_PATH,
    _artifact_dir_from_latest,
    _check,
    _mapping,
    _read_json,
    _read_jsonl,
    _read_optional_json,
    _records,
    _stable_id,
    _text,
    _unique_dir,
    _update_latest_pointer,
    _validation_payload,
)
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    SCHEMA_VERSION,
)
from ai_trading_system.etf_portfolio.dynamic_v3_pressure_validation import (
    DEFAULT_ADVISORY_OUTCOME_DIR,
    DEFAULT_BACKFILLED_OUTCOME_DIR,
    DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    DEFAULT_DEFENSIVE_PRESSURE_COMPARE_DIR,
    DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR,
    EVIDENCE_QUALITY_BY_SOURCE,
    SOURCE_MODES,
    _policy_snapshot,
    _validate_policy_live,
    _write_views_atomic,
    run_defensive_pressure_compare,
    run_pressure_outcome_backfill,
    validate_defensive_pressure_compare_artifact,
    validate_pressure_outcome_backfill_artifact,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH

CAPTURE_PLAN_SNAPSHOT_SCHEMA_VERSION = "forward_pressure_capture_input_snapshot.v2"
PRESSURE_TRIGGER_SNAPSHOT_SCHEMA_VERSION = "pressure_trigger_input_snapshot.v2"
PRESSURE_CAPTURE_SNAPSHOT_SCHEMA_VERSION = "pressure_capture_input_snapshot.v2"
PRESSURE_LEDGER_SNAPSHOT_SCHEMA_VERSION = "pressure_sample_ledger_input_snapshot.v2"
WEEKLY_DEFENSIVE_SNAPSHOT_SCHEMA_VERSION = "weekly_defensive_evidence_input_snapshot.v2"


class DynamicV3ForwardPressureError(ValueError):
    """Raised when forward pressure evidence cannot be reproduced safely."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3ForwardPressureError(message)


def _generated_at(value: datetime | None) -> datetime:
    try:
        return _operations_generated_at(value)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3ForwardPressureError(str(exc)) from exc


def _datetime(value: Any, *, field: str) -> datetime:
    try:
        return _operations_datetime(value, field=field)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3ForwardPressureError(str(exc)) from exc


def _finite(value: Any) -> bool:
    return (
        isinstance(value, int | float)
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def _read_capture_policy(path: Path) -> dict[str, Any]:
    binding = _policy_snapshot(path)
    policy = dict(_mapping(binding.get("payload")))
    _require(policy.get("schema_version") == 1, "capture policy schema invalid")
    metadata = _mapping(policy.get("policy_metadata"))
    for field in ("owner", "version", "status", "rationale", "review_condition"):
        _require(bool(_text(metadata.get(field))), f"capture policy {field} missing")
    for cadence in ("daily", "weekly"):
        section = _mapping(policy.get(cadence))
        commands = _command_strings(section.get("commands"))
        _require(section.get("enabled") is True, f"capture policy {cadence} disabled")
        _require(commands and len(commands) == len(set(commands)), f"{cadence} commands invalid")
    event = _mapping(policy.get("event_driven"))
    commands = _command_strings(event.get("commands"))
    _require(event.get("enabled") is True, "event-driven capture disabled")
    _require(commands and len(commands) == len(set(commands)), "event commands invalid")
    thresholds = _mapping(event.get("triggers"))
    required_thresholds = (
        "qqq_1w_drawdown_pct",
        "smh_1w_drawdown_pct",
        "qqq_1d_drawdown_pct",
        "smh_1d_drawdown_pct",
    )
    _require(
        all(
            _finite(thresholds.get(name)) and float(thresholds[name]) < 0
            for name in required_thresholds
        ),
        "capture trigger thresholds invalid",
    )
    floor = _mapping(policy.get("validation")).get("required_forward_pressure_samples")
    _require(
        isinstance(floor, int) and not isinstance(floor, bool) and floor > 0,
        "required forward pressure sample floor invalid",
    )
    safety = _mapping(policy.get("safety"))
    _require(
        safety.get("broker_action_allowed") is False
        and safety.get("broker_action_taken") is False
        and safety.get("production_effect") == "none"
        and safety.get("auto_apply_policy") is False
        and safety.get("policy_change_allowed") is False
        and safety.get("owner_approval_required") is True,
        "capture policy safety invalid",
    )
    return policy


def _command_strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if not isinstance(value, Sequence) or isinstance(value, bytes):
        return []
    result: list[str] = []
    for item in value:
        text = _text(item.get("command")) if isinstance(item, Mapping) else _text(item)
        if text:
            result.append(text)
    return result


def _policy_binding(path: Path) -> dict[str, Any]:
    binding = _policy_snapshot(path)
    _require(_mapping(binding.get("payload")) == _read_capture_policy(path), "policy drift")
    return binding


def _policy(snapshot: Mapping[str, Any]) -> Mapping[str, Any]:
    policies = _records(snapshot.get("policy_bindings"))
    _require(len(policies) == 1, "capture policy binding invalid")
    return _mapping(policies[0].get("payload"))


def _safety() -> dict[str, Any]:
    return {
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "policy_change_allowed": False,
        "auto_apply": False,
        "auto_policy_apply": False,
        "production_effect": "none",
        "production_candidate_generated": False,
        "manual_review_required": True,
        "owner_approval_required": True,
        "can_support_rule_approval": False,
        "research_only": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _text_bytes(value: str) -> bytes:
    return value.encode("utf-8")


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _write_views(output_dir: Path, views: Mapping[str, bytes]) -> None:
    _write_views_atomic(output_dir, views)


def _view_mismatches(output_dir: Path, views: Mapping[str, bytes]) -> list[str]:
    return [
        name for name, payload in views.items() if not _file_bytes_match(output_dir / name, payload)
    ]


def _cache_binding(path: Path, *, source_kind: str, required: bool) -> dict[str, Any]:
    if not path.is_file():
        _require(not required, f"required cache missing: {path}")
        return {
            "binding_type": "cache_file",
            "source_kind": source_kind,
            "path": str(path),
            "exists": False,
        }
    return {
        "binding_type": "cache_file",
        "source_kind": source_kind,
        "path": str(path),
        "exists": True,
        "bundle": _operations_source_bundle(
            source_dir=path.parent,
            canonical_files=(path.name,),
        ),
    }


def _validate_cache_binding_live(binding: Mapping[str, Any]) -> list[str]:
    path = Path(_text(binding.get("path")))
    if binding.get("exists") is False:
        return [f"cache appearance drift: {path}"] if path.exists() else []
    return _validate_operations_source_bundle(_mapping(binding.get("bundle")))


_SOURCE_VALIDATORS: dict[str, tuple[str, str, str, Callable[..., dict[str, Any]]]] = {}


def _source_validators() -> dict[str, tuple[str, str, str, Callable[..., dict[str, Any]]]]:
    if not _SOURCE_VALIDATORS:
        _SOURCE_VALIDATORS.update(
            {
                "pressure_trigger": (
                    "pressure_trigger_manifest.json",
                    "trigger_id",
                    "trigger_id",
                    validate_pressure_trigger_artifact,
                ),
                "pressure_regime_tag": (
                    "pressure_regime_manifest.json",
                    "tag_id",
                    "tag_id",
                    validate_pressure_regime_tag_artifact,
                ),
                "pressure_outcome_backfill": (
                    "pressure_backfill_manifest.json",
                    "pressure_backfill_id",
                    "backfill_id",
                    validate_pressure_outcome_backfill_artifact,
                ),
                "defensive_pressure_compare": (
                    "defensive_pressure_compare_manifest.json",
                    "comparison_id",
                    "comparison_id",
                    validate_defensive_pressure_compare_artifact,
                ),
                "pressure_sample_ledger": (
                    "pressure_sample_ledger_manifest.json",
                    "ledger_id",
                    "ledger_id",
                    validate_pressure_sample_ledger_artifact,
                ),
            }
        )
    return _SOURCE_VALIDATORS


def _artifact_binding(
    *,
    source_kind: str,
    source_dir: Path,
    generated: datetime,
    json_views: Sequence[str] = (),
    jsonl_views: Sequence[str] = (),
    text_views: Sequence[str] = (),
) -> dict[str, Any]:
    manifest_name, id_key, validator_key, validator = _source_validators()[source_kind]
    manifest = _read_json(source_dir / manifest_name)
    artifact_id = _text(manifest.get(id_key))
    _require(artifact_id == source_dir.name, f"{source_kind} identity mismatch")
    source_generated = _datetime(manifest.get("generated_at"), field=f"{source_kind} generated_at")
    _require(source_generated <= generated, f"{source_kind} generated after cutoff")
    validation = validator(**{validator_key: artifact_id, "output_dir": source_dir.parent})
    _require(validation.get("status") == "PASS", f"{source_kind} validation failed")
    return {
        "binding_type": "forward_pressure_artifact",
        "source_kind": source_kind,
        "artifact_id": artifact_id,
        "generated_at": source_generated.isoformat(),
        "validation": validation,
        "bundle": _operations_source_bundle(
            source_dir=source_dir,
            json_views=json_views,
            jsonl_views=jsonl_views,
            text_views=text_views,
        ),
    }


def _validate_artifact_binding_live(
    binding: Mapping[str, Any], *, generated: datetime
) -> list[str]:
    errors = _validate_operations_source_bundle(_mapping(binding.get("bundle")))
    try:
        source_kind = _text(binding.get("source_kind"))
        manifest_name, id_key, validator_key, validator = _source_validators()[source_kind]
        source_dir = Path(_text(_mapping(binding.get("bundle")).get("source_dir")))
        manifest = _read_json(source_dir / manifest_name)
        artifact_id = _text(manifest.get(id_key))
        source_generated = _datetime(
            manifest.get("generated_at"), field=f"{source_kind} generated_at"
        )
        _require(source_generated <= generated, f"{source_kind} generated after cutoff")
        validation = validator(**{validator_key: artifact_id, "output_dir": source_dir.parent})
        if validation != _mapping(binding.get("validation")):
            errors.append(f"source validation drift: {source_dir}")
        if artifact_id != binding.get("artifact_id") or artifact_id != source_dir.name:
            errors.append(f"source identity drift: {source_dir}")
        if source_generated.isoformat() != binding.get("generated_at"):
            errors.append(f"source generated_at drift: {source_dir}")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _binding(snapshot: Mapping[str, Any], kind: str) -> Mapping[str, Any]:
    matches = [
        row for row in _records(snapshot.get("source_bindings")) if row.get("source_kind") == kind
    ]
    _require(len(matches) == 1, f"snapshot source binding invalid: {kind}")
    return matches[0]


def _bundle_json(binding: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    return _mapping(_mapping(_mapping(binding.get("bundle")).get("json")).get(name))


def _bundle_jsonl(binding: Mapping[str, Any], name: str) -> list[Mapping[str, Any]]:
    return _records(_mapping(_mapping(binding.get("bundle")).get("jsonl")).get(name))


def _validate_semantic_selection(selection: Mapping[str, Any], generated: datetime) -> list[str]:
    try:
        selected = _semantic_artifact_id(
            output_dir=Path(_text(selection.get("output_dir"))),
            artifact_id=None,
            manifest_name=_text(selection.get("manifest_name")),
            id_key=_text(selection.get("id_key")),
            generated=generated,
            source_name=_text(selection.get("source_name")),
            required=selection.get("required") is True,
        )
        if selected != _text(selection.get("selected_artifact_id")):
            return [f"semantic selection drift: {selection.get('source_name')}"]
        return []
    except Exception as exc:  # noqa: BLE001
        return [str(exc)]


def _validate_snapshot_live(
    snapshot: Mapping[str, Any], *, expected_schema: str, generated: datetime
) -> list[str]:
    errors: list[str] = []
    if snapshot.get("schema_version") != expected_schema:
        errors.append("snapshot schema invalid")
    for policy in _records(snapshot.get("policy_bindings")):
        errors.extend(_validate_policy_live(policy))
        try:
            if _read_capture_policy(Path(_text(policy.get("path")))) != _mapping(
                policy.get("payload")
            ):
                errors.append("capture policy payload drift")
        except Exception as exc:  # noqa: BLE001
            errors.append(str(exc))
    for cache in _records(snapshot.get("cache_bindings")):
        errors.extend(_validate_cache_binding_live(cache))
    for source in _records(snapshot.get("source_bindings")):
        errors.extend(_validate_artifact_binding_live(source, generated=generated))
    for selection in _records(snapshot.get("semantic_selections")):
        errors.extend(_validate_semantic_selection(selection, generated))
    return errors


def _validate_recomputed_artifact(
    *,
    artifact_id: str,
    id_key: str,
    output_dir: Path,
    manifest_name: str,
    snapshot_name: str,
    snapshot_schema: str,
    view_builder: Callable[..., tuple[dict[str, bytes], dict[str, Any]]],
    report_type: str,
) -> dict[str, Any]:
    artifact_dir = output_dir / artifact_id
    manifest = _read_optional_json(artifact_dir / manifest_name) or {}
    snapshot = _read_optional_json(artifact_dir / snapshot_name) or {}
    checks = [
        _check("snapshot_exists", (artifact_dir / snapshot_name).is_file(), snapshot_name),
        _check("artifact_id_matches", manifest.get(id_key) == artifact_id, id_key),
        _check(
            "safety_research_only",
            manifest.get("can_support_rule_approval") is False
            and manifest.get("auto_apply") is False
            and manifest.get("policy_change_allowed") is False
            and manifest.get("broker_action_allowed") is False
            and manifest.get("production_effect") == "none",
            "research-only safety boundary",
        ),
    ]
    if snapshot:
        try:
            generated = _datetime(snapshot.get("generated_at"), field=f"{id_key} generated_at")
            live_errors = _validate_snapshot_live(
                snapshot, expected_schema=snapshot_schema, generated=generated
            )
            checks.append(
                _check("live_sources_config_and_cache", not live_errors, "; ".join(live_errors))
            )
            expected, _ = view_builder(snapshot, **{id_key: artifact_id}, output_dir=artifact_dir)
            mismatches = _view_mismatches(artifact_dir, expected)
            checks.append(
                _check("content_derived_views", not mismatches, f"mismatched files: {mismatches}")
            )
        except Exception as exc:  # noqa: BLE001
            checks.append(_check("content_derived_views", False, str(exc)))
    else:
        checks.append(_check("content_derived_views", False, "versioned snapshot missing"))
    return _validation_payload(
        report_type=report_type,
        artifact_id_key=id_key,
        artifact_id=artifact_id,
        checks=checks,
    )


def _capture_plan_views(
    snapshot: Mapping[str, Any], *, capture_plan_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    policy = _policy(snapshot)
    daily_pack = _command_pack("daily", policy)
    weekly_pack = _command_pack("weekly", policy)
    event_plan = _event_trigger_plan(policy)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_forward_pressure_capture_manifest",
        "capture_plan_id": capture_plan_id,
        "config_path": _text(_records(snapshot.get("policy_bindings"))[0].get("path")),
        "policy_version": _text(_mapping(policy.get("policy_metadata")).get("version")),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "market_regime": "ai_after_chatgpt",
        "capture_plan_manifest_path": str(output_dir / "capture_plan_manifest.json"),
        "daily_command_pack_path": str(output_dir / "daily_command_pack.json"),
        "weekly_command_pack_path": str(output_dir / "weekly_command_pack.json"),
        "event_driven_trigger_plan_path": str(output_dir / "event_driven_trigger_plan.json"),
        "forward_pressure_capture_report_path": str(
            output_dir / "forward_pressure_capture_report.md"
        ),
        **_safety(),
    }
    report = render_forward_pressure_capture_report(manifest, daily_pack, weekly_pack, event_plan)
    views = {
        "capture_plan_input_snapshot.json": _json_bytes(snapshot),
        "capture_plan_manifest.json": _json_bytes(manifest),
        "daily_command_pack.json": _json_bytes(daily_pack),
        "weekly_command_pack.json": _json_bytes(weekly_pack),
        "event_driven_trigger_plan.json": _json_bytes(event_plan),
        "forward_pressure_capture_report.md": _text_bytes(report),
    }
    return views, {
        "manifest": manifest,
        "daily_command_pack": daily_pack,
        "weekly_command_pack": weekly_pack,
        "event_driven_trigger_plan": event_plan,
    }


def build_forward_pressure_capture_plan(
    *,
    config_path: Path = DEFAULT_FORWARD_PRESSURE_CAPTURE_CONFIG_PATH,
    output_dir: Path = DEFAULT_FORWARD_PRESSURE_CAPTURE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    policy_binding = _policy_binding(config_path)
    capture_plan_id = _stable_id(
        "forward-pressure-capture-plan", str(config_path), generated.isoformat()
    )
    plan_dir = _unique_dir(output_dir / capture_plan_id)
    snapshot = {
        "schema_version": CAPTURE_PLAN_SNAPSHOT_SCHEMA_VERSION,
        "capture_plan_id": plan_dir.name,
        "generated_at": generated.isoformat(),
        "policy_bindings": [policy_binding],
        "cache_bindings": [],
        "source_bindings": [],
        "semantic_selections": [],
        "production_effect": "none",
    }
    views, payload = _capture_plan_views(
        snapshot, capture_plan_id=plan_dir.name, output_dir=plan_dir
    )
    _write_views(plan_dir, views)
    _update_latest_pointer(
        "latest_forward_pressure_capture",
        plan_dir.name,
        plan_dir / "capture_plan_manifest.json",
    )
    return {
        "capture_plan_id": plan_dir.name,
        "capture_plan_dir": plan_dir,
        **payload,
    }


def forward_pressure_capture_report_payload(
    *,
    capture_plan_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FORWARD_PRESSURE_CAPTURE_DIR,
) -> dict[str, Any]:
    plan_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=capture_plan_id if not latest else None,
        pointer_name="latest_forward_pressure_capture",
    )
    return {
        **_read_json(plan_dir / "capture_plan_manifest.json"),
        "daily_command_pack": _read_json(plan_dir / "daily_command_pack.json"),
        "weekly_command_pack": _read_json(plan_dir / "weekly_command_pack.json"),
        "event_driven_trigger_plan": _read_json(plan_dir / "event_driven_trigger_plan.json"),
        "capture_plan_dir": str(plan_dir),
        **_report_input_snapshot(plan_dir / "capture_plan_input_snapshot.json"),
    }


def validate_forward_pressure_capture_artifact(
    *,
    capture_plan_id: str,
    output_dir: Path = DEFAULT_FORWARD_PRESSURE_CAPTURE_DIR,
) -> dict[str, Any]:
    return _validate_recomputed_artifact(
        artifact_id=capture_plan_id,
        id_key="capture_plan_id",
        output_dir=output_dir,
        manifest_name="capture_plan_manifest.json",
        snapshot_name="capture_plan_input_snapshot.json",
        snapshot_schema=CAPTURE_PLAN_SNAPSHOT_SCHEMA_VERSION,
        view_builder=_capture_plan_views,
        report_type="etf_dynamic_v3_forward_pressure_capture_validation",
    )


def _pressure_trigger_views(
    snapshot: Mapping[str, Any], *, trigger_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    try:
        as_of = date.fromisoformat(_text(snapshot.get("as_of")))
    except ValueError as exc:
        raise DynamicV3ForwardPressureError("pressure trigger snapshot as_of invalid") from exc
    price_bindings = [
        row
        for row in _records(snapshot.get("cache_bindings"))
        if row.get("source_kind") == "etf_prices"
    ]
    _require(len(price_bindings) == 1, "pressure trigger price binding invalid")
    metrics = _trigger_metrics(
        as_of=as_of,
        prices_path=Path(_text(price_bindings[0].get("path"))),
    )
    metrics["trigger_status"] = _trigger_status(metrics, _policy(snapshot))
    actions = _triggered_actions(metrics, _policy(snapshot))
    calculation = _mapping(snapshot.get("calculation"))
    _require(
        calculation
        == {
            "trigger_metrics": metrics,
            "triggered_actions": actions,
        },
        "pressure trigger calculation snapshot mismatch",
    )
    _require(
        metrics.get("trigger_status") in {"NO_TRIGGER", "PRESSURE_TRIGGERED", "INSUFFICIENT_DATA"},
        "trigger status invalid",
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_trigger_manifest",
        "trigger_id": trigger_id,
        "as_of": snapshot.get("as_of"),
        "config_path": _text(_records(snapshot.get("policy_bindings"))[0].get("path")),
        "policy_version": _text(_mapping(_policy(snapshot).get("policy_metadata")).get("version")),
        "generated_at": snapshot.get("generated_at"),
        "status": (
            "PASS" if metrics.get("trigger_status") != "INSUFFICIENT_DATA" else "INSUFFICIENT_DATA"
        ),
        "data_quality_status": snapshot.get("data_quality_status"),
        "data_quality_report_path": str(output_dir / "validate_data_quality_report.md"),
        "market_regime": "ai_after_chatgpt",
        "pressure_trigger_manifest_path": str(output_dir / "pressure_trigger_manifest.json"),
        "trigger_metrics_path": str(output_dir / "trigger_metrics.json"),
        "triggered_actions_path": str(output_dir / "triggered_actions.json"),
        "pressure_trigger_report_path": str(output_dir / "pressure_trigger_report.md"),
        **_safety(),
    }
    report = render_pressure_trigger_report(manifest, metrics, actions)
    views = {
        "pressure_trigger_input_snapshot.json": _json_bytes(snapshot),
        "pressure_trigger_manifest.json": _json_bytes(manifest),
        "trigger_metrics.json": _json_bytes(metrics),
        "triggered_actions.json": _json_bytes(actions),
        "validate_data_quality_report.md": _text_bytes(
            _text(snapshot.get("data_quality_report_text"))
        ),
        "pressure_trigger_report.md": _text_bytes(report),
    }
    return views, {
        "manifest": manifest,
        "trigger_metrics": metrics,
        "triggered_actions": actions,
    }


def run_pressure_trigger_scan(
    *,
    as_of: date,
    config_path: Path = DEFAULT_FORWARD_PRESSURE_CAPTURE_CONFIG_PATH,
    output_dir: Path = DEFAULT_PRESSURE_TRIGGER_DIR,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    enforce_data_quality_gate: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    _require(as_of <= generated.date(), "pressure trigger as_of is after generated_at")
    policy_binding = _policy_binding(config_path)
    policy = _mapping(policy_binding.get("payload"))

    # The required quality gate runs in an isolated staging directory so a failure
    # cannot leave a formal artifact that appears current or consumable.
    with tempfile.TemporaryDirectory(prefix="aits-pressure-trigger-dq-") as temp_dir:
        staged_report = Path(temp_dir) / "validate_data_quality_report.md"
        dq_status, _ = _quality_gate_for_pressure_trigger(
            as_of=as_of,
            generated=generated,
            prices_path=prices_path,
            rates_path=rates_path,
            report_path=staged_report,
            enforce=enforce_data_quality_gate,
        )
        dq_text = _read_text(staged_report) if staged_report.is_file() else ""
    _require(
        (not enforce_data_quality_gate) or dq_status in {"PASS", "PASS_WITH_WARNINGS"},
        "pressure trigger data quality gate did not pass",
    )
    if not enforce_data_quality_gate:
        _require(
            dq_status == "SKIPPED_EXPLICIT_TEST_FIXTURE",
            "data quality skip must be an explicit test-fixture skip",
        )
        dq_text = (
            "# Data Quality Gate\n\n"
            "- status: `SKIPPED_EXPLICIT_TEST_FIXTURE`\n"
            "- scope: explicit test fixture only\n"
            "- production_effect: `none`\n"
        )

    metrics = _trigger_metrics(as_of=as_of, prices_path=prices_path)
    metrics["trigger_status"] = _trigger_status(metrics, policy)
    metric_values = _mapping(metrics.get("metrics"))
    _require(
        all(_finite(value) for value in metric_values.values()),
        "pressure trigger metrics contain non-finite values",
    )
    actions = _triggered_actions(metrics, policy)
    trigger_id = _stable_id("pressure-trigger", as_of.isoformat(), generated.isoformat())
    trigger_dir = _unique_dir(output_dir / trigger_id)
    snapshot = {
        "schema_version": PRESSURE_TRIGGER_SNAPSHOT_SCHEMA_VERSION,
        "trigger_id": trigger_dir.name,
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "data_quality_status": dq_status,
        "data_quality_gate_enforced": bool(enforce_data_quality_gate),
        "data_quality_report_text": dq_text,
        "policy_bindings": [policy_binding],
        "cache_bindings": [
            _cache_binding(prices_path, source_kind="etf_prices", required=True),
            _cache_binding(
                rates_path,
                source_kind="rates",
                required=bool(enforce_data_quality_gate),
            ),
        ],
        "source_bindings": [],
        "semantic_selections": [],
        "calculation": {
            "trigger_metrics": metrics,
            "triggered_actions": actions,
        },
        "production_effect": "none",
    }
    views, payload = _pressure_trigger_views(
        snapshot, trigger_id=trigger_dir.name, output_dir=trigger_dir
    )
    _write_views(trigger_dir, views)
    _update_latest_pointer(
        "latest_pressure_trigger",
        trigger_dir.name,
        trigger_dir / "pressure_trigger_manifest.json",
    )
    return {
        "trigger_id": trigger_dir.name,
        "trigger_dir": trigger_dir,
        **payload,
    }


def pressure_trigger_report_payload(
    *,
    trigger_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PRESSURE_TRIGGER_DIR,
) -> dict[str, Any]:
    trigger_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=trigger_id if not latest else None,
        pointer_name="latest_pressure_trigger",
    )
    return {
        **_read_json(trigger_dir / "pressure_trigger_manifest.json"),
        "trigger_metrics": _read_json(trigger_dir / "trigger_metrics.json"),
        "triggered_actions": _read_json(trigger_dir / "triggered_actions.json"),
        "trigger_dir": str(trigger_dir),
        **_report_input_snapshot(trigger_dir / "pressure_trigger_input_snapshot.json"),
    }


def validate_pressure_trigger_artifact(
    *, trigger_id: str, output_dir: Path = DEFAULT_PRESSURE_TRIGGER_DIR
) -> dict[str, Any]:
    return _validate_recomputed_artifact(
        artifact_id=trigger_id,
        id_key="trigger_id",
        output_dir=output_dir,
        manifest_name="pressure_trigger_manifest.json",
        snapshot_name="pressure_trigger_input_snapshot.json",
        snapshot_schema=PRESSURE_TRIGGER_SNAPSHOT_SCHEMA_VERSION,
        view_builder=_pressure_trigger_views,
        report_type="etf_dynamic_v3_pressure_trigger_validation",
    )


def _capture_step(step: str, status: str, artifact_id: str, **extra: Any) -> dict[str, Any]:
    return {
        "step": step,
        "status": status,
        "artifact_id": artifact_id,
        "broker_action_allowed": False,
        "production_effect": "none",
        **extra,
    }


def _pressure_capture_views(
    snapshot: Mapping[str, Any], *, capture_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    trigger = _binding(snapshot, "pressure_trigger")
    trigger_manifest = _bundle_json(trigger, "pressure_trigger_manifest.json")
    trigger_metrics = _bundle_json(trigger, "trigger_metrics.json")
    trigger_id = _text(trigger.get("artifact_id"))
    trigger_status = _text(trigger_metrics.get("trigger_status"))
    manual_force = snapshot.get("manual_force") is True
    should_run = manual_force or trigger_status == "PRESSURE_TRIGGERED"
    lineage = _mapping(snapshot.get("lineage"))
    _require(lineage.get("trigger_id") == trigger_id, "capture trigger lineage mismatch")
    _require(trigger_manifest.get("trigger_id") == trigger_id, "trigger manifest mismatch")

    if should_run:
        tag = _binding(snapshot, "pressure_regime_tag")
        backfill = _binding(snapshot, "pressure_outcome_backfill")
        comparison = _binding(snapshot, "defensive_pressure_compare")
        tag_id = _text(tag.get("artifact_id"))
        backfill_id = _text(backfill.get("artifact_id"))
        comparison_id = _text(comparison.get("artifact_id"))
        backfill_manifest = _bundle_json(backfill, "pressure_backfill_manifest.json")
        comparison_manifest = _bundle_json(comparison, "defensive_pressure_compare_manifest.json")
        _require(
            backfill_manifest.get("source_pressure_tag_id") == tag_id,
            "capture Tag-to-Backfill lineage mismatch",
        )
        _require(
            comparison_manifest.get("pressure_backfill_id") == backfill_id,
            "capture Backfill-to-Compare lineage mismatch",
        )
        _require(
            lineage
            == {
                "trigger_id": trigger_id,
                "pressure_regime_tag_id": tag_id,
                "pressure_backfill_id": backfill_id,
                "comparison_id": comparison_id,
            },
            "capture frozen lineage mismatch",
        )
        status = "PASS"
        steps = [
            _capture_step("pressure-regime-tag", "PASS", tag_id),
            _capture_step(
                "pressure-outcome-backfill",
                _text(backfill_manifest.get("status")) or "PASS",
                backfill_id,
            ),
            _capture_step("defensive-pressure-compare", "PASS", comparison_id),
        ]
    else:
        _require(
            lineage
            == {
                "trigger_id": trigger_id,
                "pressure_regime_tag_id": "",
                "pressure_backfill_id": "",
                "comparison_id": "",
            },
            "skipped capture lineage mismatch",
        )
        tag_id = backfill_id = comparison_id = ""
        status = "SKIPPED"
        steps = [
            _capture_step("pressure-regime-tag", "SKIPPED", "", reason="no_trigger"),
            _capture_step("pressure-outcome-backfill", "SKIPPED", "", reason="no_trigger"),
            _capture_step("defensive-pressure-compare", "SKIPPED", "", reason="no_trigger"),
        ]
    capture_steps = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_capture_steps",
        "trigger_id": trigger_id,
        "trigger_status": trigger_status,
        "manual_force": manual_force,
        "steps": steps,
        **_safety(),
    }
    artifacts = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_capture_artifacts",
        "trigger_id": trigger_id,
        "trigger_status": trigger_status,
        "manual_force": manual_force,
        "pressure_regime_tag_id": tag_id,
        "pressure_backfill_id": backfill_id,
        "comparison_id": comparison_id,
        **_safety(),
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_capture_manifest",
        "capture_id": capture_id,
        "trigger_id": trigger_id,
        "generated_at": snapshot.get("generated_at"),
        "status": status,
        "manual_force": manual_force,
        "market_regime": "ai_after_chatgpt",
        "pressure_capture_manifest_path": str(output_dir / "pressure_capture_manifest.json"),
        "pressure_capture_steps_path": str(output_dir / "pressure_capture_steps.json"),
        "pressure_capture_artifacts_path": str(output_dir / "pressure_capture_artifacts.json"),
        "pressure_capture_report_path": str(output_dir / "pressure_capture_report.md"),
        **_safety(),
    }
    report = render_pressure_capture_report(manifest, capture_steps, artifacts)
    views = {
        "pressure_capture_input_snapshot.json": _json_bytes(snapshot),
        "pressure_capture_manifest.json": _json_bytes(manifest),
        "pressure_capture_steps.json": _json_bytes(capture_steps),
        "pressure_capture_artifacts.json": _json_bytes(artifacts),
        "pressure_capture_report.md": _text_bytes(report),
    }
    return views, {
        "manifest": manifest,
        "pressure_capture_steps": capture_steps,
        "pressure_capture_artifacts": artifacts,
    }


def run_pressure_capture_workflow(
    *,
    trigger_id: str,
    force: bool = False,
    trigger_dir: Path = DEFAULT_PRESSURE_TRIGGER_DIR,
    output_dir: Path = DEFAULT_PRESSURE_CAPTURE_DIR,
    pressure_tag_dir: Path = DEFAULT_PRESSURE_REGIME_TAG_DIR,
    pressure_backfill_dir: Path = DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR,
    comparison_dir: Path = DEFAULT_DEFENSIVE_PRESSURE_COMPARE_DIR,
    advisory_outcome_dir: Path = DEFAULT_ADVISORY_OUTCOME_DIR,
    backfilled_outcome_dir: Path = DEFAULT_BACKFILLED_OUTCOME_DIR,
    backtest_sim_outcome_dir: Path = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    enforce_data_quality_gate: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    trigger = _artifact_binding(
        source_kind="pressure_trigger",
        source_dir=trigger_dir / trigger_id,
        generated=generated,
        json_views=(
            "pressure_trigger_input_snapshot.json",
            "pressure_trigger_manifest.json",
            "trigger_metrics.json",
            "triggered_actions.json",
        ),
        text_views=("pressure_trigger_report.md",),
    )
    trigger_manifest = _bundle_json(trigger, "pressure_trigger_manifest.json")
    trigger_metrics = _bundle_json(trigger, "trigger_metrics.json")
    try:
        as_of = date.fromisoformat(_text(trigger_manifest.get("as_of")))
    except ValueError as exc:
        raise DynamicV3ForwardPressureError("trigger as_of invalid") from exc
    _require(as_of <= generated.date(), "trigger as_of is after capture cutoff")
    trigger_status = _text(trigger_metrics.get("trigger_status"))
    should_run = bool(force) or trigger_status == "PRESSURE_TRIGGERED"
    bindings = [trigger]
    lineage = {
        "trigger_id": trigger_id,
        "pressure_regime_tag_id": "",
        "pressure_backfill_id": "",
        "comparison_id": "",
    }
    if should_run:
        tag = run_pressure_regime_tagging(
            start=AI_AFTER_CHATGPT_START,
            end=as_of,
            output_dir=pressure_tag_dir,
            advisory_outcome_dir=advisory_outcome_dir,
            prices_path=prices_path,
            rates_path=rates_path,
            enforce_data_quality_gate=enforce_data_quality_gate,
            generated_at=generated,
        )
        tag_id = _text(tag.get("tag_id"))
        tag_binding = _artifact_binding(
            source_kind="pressure_regime_tag",
            source_dir=pressure_tag_dir / tag_id,
            generated=generated,
            json_views=(
                "pressure_regime_input_snapshot.json",
                "pressure_regime_manifest.json",
                "pressure_regime_summary.json",
            ),
            jsonl_views=("regime_window_tags.jsonl", "outcome_regime_tags.jsonl"),
            text_views=("pressure_regime_report.md",),
        )
        backfill = run_pressure_outcome_backfill(
            start=AI_AFTER_CHATGPT_START,
            end=as_of,
            output_dir=pressure_backfill_dir,
            pressure_tag_dir=pressure_tag_dir,
            advisory_outcome_dir=advisory_outcome_dir,
            backfilled_outcome_dir=backfilled_outcome_dir,
            backtest_sim_outcome_dir=backtest_sim_outcome_dir,
            generated_at=generated,
        )
        backfill_id = _text(backfill.get("pressure_backfill_id"))
        backfill_binding = _artifact_binding(
            source_kind="pressure_outcome_backfill",
            source_dir=pressure_backfill_dir / backfill_id,
            generated=generated,
            json_views=(
                "pressure_outcome_backfill_input_snapshot.json",
                "pressure_backfill_manifest.json",
                "pressure_source_summary.json",
            ),
            jsonl_views=("pressure_outcome_inventory.jsonl",),
            text_views=("pressure_backfill_report.md",),
        )
        _require(
            _bundle_json(backfill_binding, "pressure_backfill_manifest.json").get(
                "source_pressure_tag_id"
            )
            == tag_id,
            "new backfill does not descend from new pressure tag",
        )
        comparison = run_defensive_pressure_compare(
            pressure_backfill_id=backfill_id,
            backfill_dir=pressure_backfill_dir,
            output_dir=comparison_dir,
            generated_at=generated,
        )
        comparison_id = _text(comparison.get("comparison_id"))
        comparison_binding = _artifact_binding(
            source_kind="defensive_pressure_compare",
            source_dir=comparison_dir / comparison_id,
            generated=generated,
            json_views=(
                "defensive_pressure_compare_input_snapshot.json",
                "defensive_pressure_compare_manifest.json",
            ),
            text_views=("defensive_pressure_compare_report.md",),
        )
        _require(
            _bundle_json(comparison_binding, "defensive_pressure_compare_manifest.json").get(
                "pressure_backfill_id"
            )
            == backfill_id,
            "new comparison does not descend from new backfill",
        )
        bindings.extend([tag_binding, backfill_binding, comparison_binding])
        lineage.update(
            {
                "pressure_regime_tag_id": tag_id,
                "pressure_backfill_id": backfill_id,
                "comparison_id": comparison_id,
            }
        )

    capture_id = _stable_id("pressure-capture", trigger_id, bool(force), generated.isoformat())
    capture_dir = _unique_dir(output_dir / capture_id)
    snapshot = {
        "schema_version": PRESSURE_CAPTURE_SNAPSHOT_SCHEMA_VERSION,
        "capture_id": capture_dir.name,
        "generated_at": generated.isoformat(),
        "manual_force": bool(force),
        "policy_bindings": [],
        "cache_bindings": [],
        "source_bindings": bindings,
        "semantic_selections": [],
        "lineage": lineage,
        "production_effect": "none",
    }
    views, payload = _pressure_capture_views(
        snapshot, capture_id=capture_dir.name, output_dir=capture_dir
    )
    _write_views(capture_dir, views)
    _update_latest_pointer(
        "latest_pressure_capture",
        capture_dir.name,
        capture_dir / "pressure_capture_manifest.json",
    )
    return {"capture_id": capture_dir.name, "capture_dir": capture_dir, **payload}


def pressure_capture_report_payload(
    *,
    capture_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PRESSURE_CAPTURE_DIR,
) -> dict[str, Any]:
    capture_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=capture_id if not latest else None,
        pointer_name="latest_pressure_capture",
    )
    return {
        **_read_json(capture_dir / "pressure_capture_manifest.json"),
        "pressure_capture_steps": _read_json(capture_dir / "pressure_capture_steps.json"),
        "pressure_capture_artifacts": _read_json(capture_dir / "pressure_capture_artifacts.json"),
        "capture_dir": str(capture_dir),
        **_report_input_snapshot(capture_dir / "pressure_capture_input_snapshot.json"),
    }


def validate_pressure_capture_artifact(
    *, capture_id: str, output_dir: Path = DEFAULT_PRESSURE_CAPTURE_DIR
) -> dict[str, Any]:
    return _validate_recomputed_artifact(
        artifact_id=capture_id,
        id_key="capture_id",
        output_dir=output_dir,
        manifest_name="pressure_capture_manifest.json",
        snapshot_name="pressure_capture_input_snapshot.json",
        snapshot_schema=PRESSURE_CAPTURE_SNAPSHOT_SCHEMA_VERSION,
        view_builder=_pressure_capture_views,
        report_type="etf_dynamic_v3_pressure_capture_validation",
    )


def _distinct_pressure_samples(
    inventory: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in inventory:
        source_mode = _text(row.get("source_mode"))
        source_event_id = _text(row.get("source_event_id"))
        _require(source_mode in SOURCE_MODES, f"invalid pressure source mode: {source_mode}")
        _require(source_event_id, "pressure source_event_id missing")
        grouped[(source_mode, source_event_id)].append(row)
    samples: list[dict[str, Any]] = []
    for (source_mode, source_event_id), rows in sorted(grouped.items()):
        as_of_values = {_text(row.get("as_of")) for row in rows if _text(row.get("as_of"))}
        _require(len(as_of_values) == 1, "pressure event has ambiguous as_of")
        artifact_ids = {
            _text(row.get("source_artifact_id"))
            for row in rows
            if _text(row.get("source_artifact_id"))
        }
        _require(len(artifact_ids) == 1, "pressure event has ambiguous source artifact")
        relevant = any(row.get("defensive_validation_relevant") is True for row in rows)
        eligible = (
            source_mode == "FORWARD_OUTCOME"
            and relevant
            and any(row.get("can_support_production") is True for row in rows)
        )
        samples.append(
            {
                "sample_id": _stable_id("pressure-event-sample", source_mode, source_event_id),
                "source_mode": source_mode,
                "source_event_id": source_event_id,
                "source_artifact_id": next(iter(artifact_ids)),
                "as_of": next(iter(as_of_values)),
                "window_count": len(rows),
                "pressure_outcome_ids": sorted(
                    _text(row.get("pressure_outcome_id")) for row in rows
                ),
                "regime_tags": sorted(
                    {
                        _text(tag)
                        for row in rows
                        for tag in (
                            row.get("regime_tags")
                            if isinstance(row.get("regime_tags"), Sequence)
                            and not isinstance(row.get("regime_tags"), str | bytes)
                            else []
                        )
                        if _text(tag)
                    }
                ),
                "defensive_validation_relevant": relevant,
                "evidence_quality": EVIDENCE_QUALITY_BY_SOURCE.get(source_mode, "UNKNOWN"),
                "can_support_rule_approval": eligible,
                "production_effect": "none",
                "broker_action_allowed": False,
            }
        )
    identities = [_text(row.get("sample_id")) for row in samples]
    _require(len(identities) == len(set(identities)), "duplicate pressure event identity")
    return samples


def _pressure_sample_summary(
    samples: Sequence[Mapping[str, Any]], *, required_forward: int
) -> dict[str, Any]:
    source_counts = Counter(_text(row.get("source_mode")) for row in samples)
    forward = source_counts.get("FORWARD_OUTCOME", 0)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_sample_summary",
        "sample_unit": "distinct_source_mode_plus_source_event_id",
        "total_samples": len(samples),
        "forward_samples": forward,
        "pit_replay_samples": source_counts.get("HISTORICAL_REPLAY", 0),
        "simulation_samples": source_counts.get("BACKTEST_SIMULATION", 0),
        "defensive_validation_relevant": sum(
            row.get("defensive_validation_relevant") is True for row in samples
        ),
        "rule_approval_eligible_samples": sum(
            row.get("can_support_rule_approval") is True for row in samples
        ),
        "required_forward_pressure_samples": required_forward,
        "progress_to_requirement": round(min(forward / required_forward, 1.0), 6),
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _pressure_ledger_views(
    snapshot: Mapping[str, Any], *, ledger_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    selected = _text(_records(snapshot.get("semantic_selections"))[0].get("selected_artifact_id"))
    inventory: list[Mapping[str, Any]] = []
    if selected:
        backfill = _binding(snapshot, "pressure_outcome_backfill")
        _require(backfill.get("artifact_id") == selected, "ledger source selection mismatch")
        inventory = _bundle_jsonl(backfill, "pressure_outcome_inventory.jsonl")
    else:
        _require(not _records(snapshot.get("source_bindings")), "empty ledger has sources")
    samples = _distinct_pressure_samples(inventory)
    required_forward = int(
        _mapping(_policy(snapshot).get("validation")).get("required_forward_pressure_samples")
    )
    summary = _pressure_sample_summary(samples, required_forward=required_forward)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_sample_ledger_manifest",
        "ledger_id": ledger_id,
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS" if samples else "PASS_WITH_WARNINGS",
        "source_pressure_backfill_id": selected,
        "market_regime": "ai_after_chatgpt",
        "pressure_sample_ledger_manifest_path": str(
            output_dir / "pressure_sample_ledger_manifest.json"
        ),
        "pressure_samples_path": str(output_dir / "pressure_samples.jsonl"),
        "pressure_sample_summary_path": str(output_dir / "pressure_sample_summary.json"),
        "pressure_sample_ledger_report_path": str(output_dir / "pressure_sample_ledger_report.md"),
        **_safety(),
    }
    report = render_pressure_sample_ledger_report(manifest, summary)
    views = {
        "pressure_sample_ledger_input_snapshot.json": _json_bytes(snapshot),
        "pressure_sample_ledger_manifest.json": _json_bytes(manifest),
        "pressure_samples.jsonl": _jsonl_bytes(samples),
        "pressure_sample_summary.json": _json_bytes(summary),
        "pressure_sample_ledger_report.md": _text_bytes(report),
    }
    return views, {
        "manifest": manifest,
        "pressure_samples": samples,
        "pressure_sample_summary": summary,
    }


def update_pressure_sample_ledger(
    *,
    output_dir: Path = DEFAULT_PRESSURE_SAMPLE_LEDGER_DIR,
    pressure_backfill_dir: Path = DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR,
    config_path: Path = DEFAULT_FORWARD_PRESSURE_CAPTURE_CONFIG_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    try:
        selected = _semantic_artifact_id(
            output_dir=pressure_backfill_dir,
            artifact_id=None,
            manifest_name="pressure_backfill_manifest.json",
            id_key="pressure_backfill_id",
            generated=generated,
            source_name="pressure outcome backfill",
            required=False,
        )
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3ForwardPressureError(str(exc)) from exc
    source_bindings: list[dict[str, Any]] = []
    if selected:
        source_bindings.append(
            _artifact_binding(
                source_kind="pressure_outcome_backfill",
                source_dir=pressure_backfill_dir / selected,
                generated=generated,
                json_views=(
                    "pressure_outcome_backfill_input_snapshot.json",
                    "pressure_backfill_manifest.json",
                    "pressure_source_summary.json",
                ),
                jsonl_views=("pressure_outcome_inventory.jsonl",),
                text_views=("pressure_backfill_report.md",),
            )
        )
    ledger_id = _stable_id("pressure-sample-ledger", generated.isoformat(), selected)
    ledger_dir = _unique_dir(output_dir / ledger_id)
    selection = {
        "output_dir": str(pressure_backfill_dir),
        "manifest_name": "pressure_backfill_manifest.json",
        "id_key": "pressure_backfill_id",
        "source_name": "pressure outcome backfill",
        "required": False,
        "selected_artifact_id": selected,
        "selection_rule": "generated_at_then_artifact_id_at_or_before_cutoff",
    }
    snapshot = {
        "schema_version": PRESSURE_LEDGER_SNAPSHOT_SCHEMA_VERSION,
        "ledger_id": ledger_dir.name,
        "generated_at": generated.isoformat(),
        "policy_bindings": [_policy_binding(config_path)],
        "cache_bindings": [],
        "source_bindings": source_bindings,
        "semantic_selections": [selection],
        "production_effect": "none",
    }
    views, payload = _pressure_ledger_views(
        snapshot, ledger_id=ledger_dir.name, output_dir=ledger_dir
    )
    _write_views(ledger_dir, views)
    _update_latest_pointer(
        "latest_pressure_sample_ledger",
        ledger_dir.name,
        ledger_dir / "pressure_sample_ledger_manifest.json",
    )
    return {"ledger_id": ledger_dir.name, "ledger_dir": ledger_dir, **payload}


def pressure_sample_ledger_report_payload(
    *,
    ledger_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PRESSURE_SAMPLE_LEDGER_DIR,
) -> dict[str, Any]:
    ledger_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=ledger_id if not latest else None,
        pointer_name="latest_pressure_sample_ledger",
    )
    return {
        **_read_json(ledger_dir / "pressure_sample_ledger_manifest.json"),
        "pressure_samples": _read_jsonl(ledger_dir / "pressure_samples.jsonl"),
        "pressure_sample_summary": _read_json(ledger_dir / "pressure_sample_summary.json"),
        "ledger_dir": str(ledger_dir),
        **_report_input_snapshot(ledger_dir / "pressure_sample_ledger_input_snapshot.json"),
    }


def validate_pressure_sample_ledger_artifact(
    *, ledger_id: str, output_dir: Path = DEFAULT_PRESSURE_SAMPLE_LEDGER_DIR
) -> dict[str, Any]:
    return _validate_recomputed_artifact(
        artifact_id=ledger_id,
        id_key="ledger_id",
        output_dir=output_dir,
        manifest_name="pressure_sample_ledger_manifest.json",
        snapshot_name="pressure_sample_ledger_input_snapshot.json",
        snapshot_schema=PRESSURE_LEDGER_SNAPSHOT_SCHEMA_VERSION,
        view_builder=_pressure_ledger_views,
        report_type="etf_dynamic_v3_pressure_sample_ledger_validation",
    )


def _weekly_defensive_summary(
    *,
    week_ending: date,
    samples: Sequence[Mapping[str, Any]],
    ledger_summary: Mapping[str, Any],
) -> dict[str, Any]:
    week_start = week_ending - timedelta(days=6)
    weekly: list[Mapping[str, Any]] = []
    for row in samples:
        try:
            sample_date = date.fromisoformat(_text(row.get("as_of")))
        except ValueError as exc:
            raise DynamicV3ForwardPressureError("pressure sample as_of invalid") from exc
        if week_start <= sample_date <= week_ending:
            weekly.append(row)
    counts = Counter(_text(row.get("source_mode")) for row in weekly)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_defensive_summary",
        "sample_unit": "distinct_source_mode_plus_source_event_id",
        "week_ending": week_ending.isoformat(),
        "new_forward_pressure_samples": counts.get("FORWARD_OUTCOME", 0),
        "new_pit_pressure_samples": counts.get("HISTORICAL_REPLAY", 0),
        "new_simulation_pressure_samples": counts.get("BACKTEST_SIMULATION", 0),
        "total_forward_pressure_samples": int(ledger_summary.get("forward_samples", 0)),
        "required_forward_pressure_samples": int(
            ledger_summary.get("required_forward_pressure_samples", 0)
        ),
        "defensive_rule_status": "RESEARCH_ONLY",
        "weekly_recommendation": "continue_tracking",
        "owner_action_required": False,
        "policy_change_allowed": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _weekly_defensive_views(
    snapshot: Mapping[str, Any], *, weekly_defensive_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    generated = _datetime(snapshot.get("generated_at"), field="weekly generated_at")
    try:
        week_ending = date.fromisoformat(_text(snapshot.get("week_ending")))
    except ValueError as exc:
        raise DynamicV3ForwardPressureError("week_ending invalid") from exc
    _require(week_ending <= generated.date(), "week_ending is after generated_at")
    ledger = _binding(snapshot, "pressure_sample_ledger")
    selected = _text(_records(snapshot.get("semantic_selections"))[0].get("selected_artifact_id"))
    _require(ledger.get("artifact_id") == selected, "weekly ledger selection mismatch")
    samples = _bundle_jsonl(ledger, "pressure_samples.jsonl")
    ledger_summary = _bundle_json(ledger, "pressure_sample_summary.json")
    summary = _weekly_defensive_summary(
        week_ending=week_ending,
        samples=samples,
        ledger_summary=ledger_summary,
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_defensive_manifest",
        "weekly_defensive_id": weekly_defensive_id,
        "week_ending": week_ending.isoformat(),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "source_ledger_id": selected,
        "market_regime": "ai_after_chatgpt",
        "weekly_defensive_manifest_path": str(output_dir / "weekly_defensive_manifest.json"),
        "weekly_defensive_summary_path": str(output_dir / "weekly_defensive_summary.json"),
        "weekly_defensive_report_path": str(output_dir / "weekly_defensive_report.md"),
        "reader_brief_section_path": str(output_dir / "reader_brief_section.md"),
        **_safety(),
    }
    report = render_weekly_defensive_report(manifest, summary)
    reader_brief = render_weekly_defensive_reader_brief(summary)
    views = {
        "weekly_defensive_evidence_input_snapshot.json": _json_bytes(snapshot),
        "weekly_defensive_manifest.json": _json_bytes(manifest),
        "weekly_defensive_summary.json": _json_bytes(summary),
        "weekly_defensive_report.md": _text_bytes(report),
        "reader_brief_section.md": _text_bytes(reader_brief),
    }
    return views, {
        "manifest": manifest,
        "weekly_defensive_summary": summary,
        "reader_brief_section": reader_brief,
    }


def run_weekly_defensive_evidence_update(
    *,
    week_ending: date,
    output_dir: Path = DEFAULT_WEEKLY_DEFENSIVE_EVIDENCE_DIR,
    ledger_dir: Path = DEFAULT_PRESSURE_SAMPLE_LEDGER_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    _require(week_ending <= generated.date(), "week_ending is after generated_at")
    try:
        selected = _semantic_artifact_id(
            output_dir=ledger_dir,
            artifact_id=None,
            manifest_name="pressure_sample_ledger_manifest.json",
            id_key="ledger_id",
            generated=generated,
            source_name="pressure sample ledger",
            required=True,
        )
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3ForwardPressureError(str(exc)) from exc
    ledger_binding = _artifact_binding(
        source_kind="pressure_sample_ledger",
        source_dir=ledger_dir / selected,
        generated=generated,
        json_views=(
            "pressure_sample_ledger_input_snapshot.json",
            "pressure_sample_ledger_manifest.json",
            "pressure_sample_summary.json",
        ),
        jsonl_views=("pressure_samples.jsonl",),
        text_views=("pressure_sample_ledger_report.md",),
    )
    weekly_id = _stable_id(
        "weekly-defensive-evidence", week_ending.isoformat(), generated.isoformat()
    )
    weekly_dir = _unique_dir(output_dir / weekly_id)
    snapshot = {
        "schema_version": WEEKLY_DEFENSIVE_SNAPSHOT_SCHEMA_VERSION,
        "weekly_defensive_id": weekly_dir.name,
        "week_ending": week_ending.isoformat(),
        "generated_at": generated.isoformat(),
        "policy_bindings": [],
        "cache_bindings": [],
        "source_bindings": [ledger_binding],
        "semantic_selections": [
            {
                "output_dir": str(ledger_dir),
                "manifest_name": "pressure_sample_ledger_manifest.json",
                "id_key": "ledger_id",
                "source_name": "pressure sample ledger",
                "required": True,
                "selected_artifact_id": selected,
                "selection_rule": "generated_at_then_artifact_id_at_or_before_cutoff",
            }
        ],
        "production_effect": "none",
    }
    views, payload = _weekly_defensive_views(
        snapshot,
        weekly_defensive_id=weekly_dir.name,
        output_dir=weekly_dir,
    )
    _write_views(weekly_dir, views)
    _update_latest_pointer(
        "latest_weekly_defensive_evidence",
        weekly_dir.name,
        weekly_dir / "weekly_defensive_manifest.json",
    )
    return {
        "weekly_defensive_id": weekly_dir.name,
        "weekly_defensive_dir": weekly_dir,
        **payload,
    }


def weekly_defensive_evidence_report_payload(
    *,
    weekly_defensive_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEEKLY_DEFENSIVE_EVIDENCE_DIR,
) -> dict[str, Any]:
    weekly_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=weekly_defensive_id if not latest else None,
        pointer_name="latest_weekly_defensive_evidence",
    )
    return {
        **_read_json(weekly_dir / "weekly_defensive_manifest.json"),
        "weekly_defensive_summary": _read_json(weekly_dir / "weekly_defensive_summary.json"),
        "reader_brief_section": _read_text(weekly_dir / "reader_brief_section.md"),
        "weekly_defensive_dir": str(weekly_dir),
        **_report_input_snapshot(weekly_dir / "weekly_defensive_evidence_input_snapshot.json"),
    }


def validate_weekly_defensive_evidence_artifact(
    *,
    weekly_defensive_id: str,
    output_dir: Path = DEFAULT_WEEKLY_DEFENSIVE_EVIDENCE_DIR,
) -> dict[str, Any]:
    return _validate_recomputed_artifact(
        artifact_id=weekly_defensive_id,
        id_key="weekly_defensive_id",
        output_dir=output_dir,
        manifest_name="weekly_defensive_manifest.json",
        snapshot_name="weekly_defensive_evidence_input_snapshot.json",
        snapshot_schema=WEEKLY_DEFENSIVE_SNAPSHOT_SCHEMA_VERSION,
        view_builder=_weekly_defensive_views,
        report_type="etf_dynamic_v3_weekly_defensive_evidence_validation",
    )
