from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pandas as pd
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_cycle import (
    DEFAULT_CONFIRMATION_EVALUATION_DIR,
    DEFAULT_CONFIRMATION_PROGRESS_DIR,
    DEFAULT_CONFIRMATION_REGISTRY_DIR,
    DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH,
    DEFAULT_RULE_REVIEW_CYCLE_DIR,
    list_rule_owner_decisions,
    run_confirmation_evaluation,
    run_rule_review_cycle,
    update_confirmation_progress,
    validate_confirmation_evaluation_artifact,
    validate_confirmation_progress_artifact,
    validate_confirmation_targets_artifact,
    validate_rule_owner_decision_artifact,
    validate_rule_review_cycle_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    _artifact_dir_from_latest,
    _check,
    _float,
    _int,
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
    _write_jsonl,
)
from ai_trading_system.etf_portfolio.dynamic_v3_outcome_accumulation import (
    DEFAULT_CONSENSUS_RISK_DIR,
    DEFAULT_EVIDENCE_TREND_DIR,
    DEFAULT_FORWARD_OUTCOME_DECISION_DIR,
    DEFAULT_LIMITED_VS_NOTRADE_DIR,
    DEFAULT_OUTCOME_DUE_DIR,
    DEFAULT_OUTCOME_UPDATE_DIR,
    DEFAULT_OUTCOME_UPDATE_REVIEW_DIR,
    DEFAULT_ROLLING_EVIDENCE_REFRESH_DIR,
    run_evidence_trend,
    run_forward_outcome_decision,
    run_outcome_due_scan,
    run_outcome_update,
    run_outcome_update_review,
    run_rolling_evidence_refresh,
    validate_evidence_trend_artifact,
    validate_forward_outcome_decision_artifact,
    validate_outcome_due_artifact,
    validate_outcome_update_artifact,
    validate_outcome_update_review_artifact,
    validate_rolling_evidence_refresh_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DEFAULT_ADVISORY_OUTCOME_DIR,
    DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    DEFAULT_RATES_CACHE_PATH,
    validate_advisory_outcome_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_CONSENSUS_DRIFT_DIR,
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
    DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    DEFAULT_SHADOW_MONITOR_RUN_DIR,
    DEFAULT_SHADOW_SHORTLIST_DIR,
    DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    SCHEMA_VERSION,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH
from ai_trading_system.platform.artifacts.validation_session import (
    with_artifact_validation_session,
)
from ai_trading_system.platform.artifacts.writer import (
    write_bytes_atomic,
    write_json_atomic,
    write_text_atomic,
)

DEFAULT_CONFIRMATION_CYCLE_SCHEDULE_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "confirmation_cycle_schedule_v1.yaml"
)
DEFAULT_PRESSURE_REGIME_TAGGING_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "pressure_regime_tagging_v1.yaml"
)
DEFAULT_CONFIRMATION_CYCLE_PLAN_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "confirmation_cycle_plan"
DEFAULT_CONFIRMATION_CYCLE_WEEKLY_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "confirmation_cycle_weekly"
)
DEFAULT_PRESSURE_REGIME_TAG_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "pressure_regime_tag"
DEFAULT_CONFIRMATION_DASHBOARD_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "confirmation_dashboard"
DEFAULT_RULE_REVIEW_QUEUE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "rule_review_queue"

PRESSURE_TAGS = (
    "tech_drawdown",
    "risk_off",
    "semiconductor_pullback",
    "sideways_choppy",
    "strong_recovery",
    "ai_trend",
)
PRESSURE_VALIDATION_TAGS = {"tech_drawdown", "risk_off", "semiconductor_pullback"}
REQUIRED_CONFIRMATION_CYCLE_STEPS = (
    "outcome_due_scan",
    "outcome_update_review",
    "outcome_update_if_ready",
    "rolling_evidence_refresh",
    "confirmation_progress_update",
    "confirmation_evaluate",
    "rule_review_cycle",
    "owner_decision_queue_update",
    "weekly_dashboard",
    "reader_brief_update",
)
CONFIRMATION_CYCLE_PLAN_SNAPSHOT_SCHEMA_VERSION = "confirmation_cycle_plan_input_snapshot.v2"
CONFIRMATION_CYCLE_WEEKLY_SNAPSHOT_SCHEMA_VERSION = "confirmation_cycle_weekly_input_snapshot.v2"
PRESSURE_REGIME_SNAPSHOT_SCHEMA_VERSION = "pressure_regime_input_snapshot.v2"
CONFIRMATION_DASHBOARD_SNAPSHOT_SCHEMA_VERSION = "confirmation_dashboard_input_snapshot.v2"
RULE_REVIEW_QUEUE_SNAPSHOT_SCHEMA_VERSION = "rule_review_queue_input_snapshot.v2"


class DynamicV3ConfirmationOperationsError(ValueError):
    """Raised when confirmation weekly operations artifacts fail closed."""


def _operations_generated_at(value: datetime | None) -> datetime:
    generated = value or datetime.now(UTC)
    if generated.tzinfo is None or generated.utcoffset() is None:
        raise DynamicV3ConfirmationOperationsError("generated_at must be timezone-aware")
    return generated.astimezone(UTC)


def _operations_datetime(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(_text(value))
        except ValueError as exc:
            raise DynamicV3ConfirmationOperationsError(f"{field} must be ISO datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise DynamicV3ConfirmationOperationsError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _operations_file_commitment(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise DynamicV3ConfirmationOperationsError(f"source file missing: {path}")
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return {
        "path": str(path),
        "size_bytes": path.stat().st_size,
        "sha256": digest.hexdigest(),
    }


def _operations_source_bundle(
    *,
    source_dir: Path,
    canonical_files: Sequence[str] | None = None,
    json_views: Sequence[str] = (),
    jsonl_views: Sequence[str] = (),
    text_views: Sequence[str] = (),
) -> dict[str, Any]:
    names = list(
        canonical_files or sorted(path.name for path in source_dir.iterdir() if path.is_file())
    )
    if not names or len(names) != len(set(names)):
        raise DynamicV3ConfirmationOperationsError("source bundle files missing or duplicate")
    files = {name: _operations_file_commitment(source_dir / name) for name in names}
    return {
        "schema_version": "content_commitment_bundle.v1",
        "source_dir": str(source_dir),
        "canonical_file_count": len(names),
        "files": files,
        "json": {name: _read_json(source_dir / name) for name in json_views},
        "jsonl": {name: _read_jsonl(source_dir / name) for name in jsonl_views},
        "text": {name: _read_text(source_dir / name) for name in text_views},
    }


def _validate_operations_source_bundle(bundle: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    source_dir = Path(_text(bundle.get("source_dir")))
    files = _mapping(bundle.get("files"))
    if (
        bundle.get("schema_version") != "content_commitment_bundle.v1"
        or _int(bundle.get("canonical_file_count")) != len(files)
        or not files
    ):
        errors.append("source bundle envelope invalid")
        return errors
    for name, raw in files.items():
        expected = _mapping(raw)
        path = source_dir / name
        try:
            actual = _operations_file_commitment(path)
        except DynamicV3ConfirmationOperationsError as exc:
            errors.append(str(exc))
            continue
        if actual != expected:
            errors.append(f"source commitment drift: {path}")
    for name, expected in _mapping(bundle.get("json")).items():
        try:
            if _read_json(source_dir / name) != expected:
                errors.append(f"source JSON drift: {source_dir / name}")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"source JSON invalid: {source_dir / name}: {exc}")
    for name, expected in _mapping(bundle.get("jsonl")).items():
        try:
            if _read_jsonl(source_dir / name) != expected:
                errors.append(f"source JSONL drift: {source_dir / name}")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"source JSONL invalid: {source_dir / name}: {exc}")
    for name, expected in _mapping(bundle.get("text")).items():
        try:
            if _read_text(source_dir / name) != expected:
                errors.append(f"source text drift: {source_dir / name}")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"source text invalid: {source_dir / name}: {exc}")
    return errors


def _strict_validation(validation: Mapping[str, Any], *, source_name: str) -> dict[str, Any]:
    payload = dict(validation)
    if payload.get("status") != "PASS":
        raise DynamicV3ConfirmationOperationsError(f"{source_name} validation failed")
    return payload


def _report_input_snapshot(path: Path) -> dict[str, Any]:
    snapshot = _read_optional_json(path)
    if snapshot is not None:
        return {"input_snapshot": snapshot}
    return {
        "input_snapshot": {},
        "status": "PASS_WITH_WARNINGS",
        "legacy_unsnapshotted": True,
        "current_conclusion_eligible": False,
        "warnings": [
            "legacy artifact has no versioned input snapshot; read-only display only"
        ],
    }


def _source_not_after_cutoff(
    manifest: Mapping[str, Any], *, generated: datetime, source_name: str
) -> datetime:
    source_generated = _operations_datetime(
        manifest.get("generated_at"), field=f"{source_name} generated_at"
    )
    if source_generated > generated:
        raise DynamicV3ConfirmationOperationsError(f"{source_name} generated after cutoff")
    return source_generated


def _semantic_artifact_id(
    *,
    output_dir: Path,
    artifact_id: str | None,
    manifest_name: str,
    id_key: str,
    generated: datetime,
    source_name: str,
    required: bool,
) -> str:
    if artifact_id is not None:
        if artifact_id == "" and not required:
            return ""
        manifest = _read_optional_json(output_dir / artifact_id / manifest_name)
        if not manifest:
            raise DynamicV3ConfirmationOperationsError(
                f"{source_name} artifact not found: {artifact_id}"
            )
        if manifest.get(id_key) != artifact_id:
            raise DynamicV3ConfirmationOperationsError(f"{source_name} id mismatch")
        _source_not_after_cutoff(manifest, generated=generated, source_name=source_name)
        return artifact_id
    candidates: list[tuple[datetime, str]] = []
    if output_dir.exists():
        for path in sorted(output_dir.glob(f"*/{manifest_name}")):
            manifest = _read_optional_json(path)
            if not manifest:
                raise DynamicV3ConfirmationOperationsError(
                    f"{source_name} manifest invalid: {path}"
                )
            candidate_id = _text(manifest.get(id_key))
            if not candidate_id or candidate_id != path.parent.name:
                raise DynamicV3ConfirmationOperationsError(
                    f"{source_name} manifest identity invalid: {path}"
                )
            candidate_generated = _operations_datetime(
                manifest.get("generated_at"), field=f"{source_name} generated_at"
            )
            if candidate_generated <= generated:
                candidates.append((candidate_generated, candidate_id))
    if not candidates:
        if required:
            raise DynamicV3ConfirmationOperationsError(
                f"{source_name} has no artifact at or before cutoff"
            )
        return ""
    return max(candidates, key=lambda item: (item[0], item[1]))[1]


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )


def _jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return "".join(
        json.dumps(dict(row), ensure_ascii=False, sort_keys=True) + "\n" for row in rows
    ).encode("utf-8")


def _file_bytes_match(path: Path, expected: bytes) -> bool:
    return path.is_file() and path.read_bytes() == expected


def validate_confirmation_cycle_schedule_config(
    *, config_path: Path = DEFAULT_CONFIRMATION_CYCLE_SCHEDULE_CONFIG_PATH
) -> dict[str, Any]:
    config = _read_yaml_config(config_path)
    schedule = _mapping(config.get("schedule"))
    safety = _mapping(config.get("safety"))
    execution = _mapping(config.get("execution"))
    steps = [_text(item) for item in config.get("steps", []) if _text(item)]
    checks = [
        _check("config_exists", config_path.exists(), str(config_path)),
        _check("schema_version_supported", _int(config.get("schema_version")) == 1, "schema=1"),
        _check("cadence_weekly", schedule.get("cadence") == "weekly", "cadence=weekly"),
        _check("timezone_present", bool(_text(schedule.get("timezone"))), "timezone"),
        _check(
            "required_steps_present",
            set(steps) >= set(REQUIRED_CONFIRMATION_CYCLE_STEPS),
            "required weekly steps",
        ),
        _check(
            "dry_run_default_enabled",
            execution.get("dry_run_default") is True,
            "weekly runner defaults to dry-run",
        ),
        _check(
            "explicit_update_required",
            execution.get("require_explicit_update") is True,
            "outcome update requires explicit flag",
        ),
        _check(
            "safety_no_broker",
            safety.get("broker_action_allowed") is False
            and safety.get("broker_action_taken") is False,
            "broker action disabled",
        ),
        _check(
            "safety_no_production",
            safety.get("production_effect") == "none" and safety.get("auto_apply_policy") is False,
            "no production or auto apply",
        ),
        _check(
            "owner_review_required",
            safety.get("owner_approval_required") is True,
            "owner approval required",
        ),
    ]
    payload = _validation_payload(
        report_type="etf_dynamic_v3_confirmation_cycle_schedule_config_validation",
        artifact_id_key="config_path",
        artifact_id=str(config_path),
        checks=checks,
    )
    payload["config"] = config
    return payload


def build_confirmation_cycle_plan(
    *,
    config_path: Path = DEFAULT_CONFIRMATION_CYCLE_SCHEDULE_CONFIG_PATH,
    output_dir: Path = DEFAULT_CONFIRMATION_CYCLE_PLAN_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _operations_generated_at(generated_at)
    validation = validate_confirmation_cycle_schedule_config(config_path=config_path)
    _strict_validation(validation, source_name="confirmation cycle config")
    config = _mapping(validation.get("config"))
    plan_id = _stable_id("confirmation-cycle-plan", str(config_path), generated.isoformat())
    plan_dir = _unique_dir(output_dir / plan_id)
    safety = _schedule_safety(config)
    commands = _scheduled_commands(config_path)
    command_pack = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_scheduled_command_pack",
        "plan_id": plan_dir.name,
        "cadence": _text(_mapping(config.get("schedule")).get("cadence"), "weekly"),
        "preferred_weekday": _text(_mapping(config.get("schedule")).get("preferred_weekday")),
        "timezone": _text(_mapping(config.get("schedule")).get("timezone")),
        "safety": safety,
        "commands": commands,
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_cycle_plan_manifest",
        "plan_id": plan_dir.name,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "config_path": str(config_path),
        "planned_step_count": len(commands),
        "confirmation_cycle_plan_manifest_path": str(
            plan_dir / "confirmation_cycle_plan_manifest.json"
        ),
        "scheduled_command_pack_path": str(plan_dir / "scheduled_command_pack.json"),
        "confirmation_cycle_plan_input_snapshot_path": str(
            plan_dir / "confirmation_cycle_plan_input_snapshot.json"
        ),
        "confirmation_cycle_runbook_path": str(plan_dir / "confirmation_cycle_runbook.md"),
        "confirmation_cycle_plan_report_path": str(plan_dir / "confirmation_cycle_plan_report.md"),
        "market_regime": "ai_after_chatgpt",
        **_artifact_safety(),
    }
    snapshot = {
        "schema_version": CONFIRMATION_CYCLE_PLAN_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_cycle_plan_input_snapshot",
        "generated_at": generated.isoformat(),
        "config_path": str(config_path),
        "config_commitment": _operations_file_commitment(config_path),
        "config": config,
        "config_validation": validation,
        "production_effect": "none",
    }
    runbook = render_confirmation_cycle_runbook(manifest, command_pack)
    report = render_confirmation_cycle_plan_report(manifest, command_pack)
    plan_dir.mkdir(parents=True, exist_ok=False)
    write_json_atomic(plan_dir / "confirmation_cycle_plan_manifest.json", manifest)
    write_json_atomic(plan_dir / "scheduled_command_pack.json", command_pack)
    write_json_atomic(plan_dir / "confirmation_cycle_plan_input_snapshot.json", snapshot)
    write_text_atomic(
        plan_dir / "confirmation_cycle_runbook.md",
        runbook,
    )
    write_text_atomic(
        plan_dir / "confirmation_cycle_plan_report.md",
        report,
    )
    _update_latest_pointer(
        "latest_confirmation_cycle_plan",
        plan_dir.name,
        plan_dir / "confirmation_cycle_plan_manifest.json",
    )
    return {
        "plan_id": plan_dir.name,
        "plan_dir": plan_dir,
        "manifest": manifest,
        "scheduled_command_pack": command_pack,
        "input_snapshot": snapshot,
    }


def confirmation_cycle_plan_report_payload(
    *,
    plan_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CONFIRMATION_CYCLE_PLAN_DIR,
) -> dict[str, Any]:
    plan_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=plan_id if not latest else None,
        pointer_name="latest_confirmation_cycle_plan",
    )
    return {
        **_read_json(plan_dir / "confirmation_cycle_plan_manifest.json"),
        "scheduled_command_pack": _read_json(plan_dir / "scheduled_command_pack.json"),
        **_report_input_snapshot(plan_dir / "confirmation_cycle_plan_input_snapshot.json"),
        "confirmation_cycle_runbook": _read_text(plan_dir / "confirmation_cycle_runbook.md"),
        "confirmation_cycle_plan_report": _read_text(
            plan_dir / "confirmation_cycle_plan_report.md"
        ),
        "plan_dir": str(plan_dir),
    }


def validate_confirmation_cycle_plan_artifact(
    *, plan_id: str, output_dir: Path = DEFAULT_CONFIRMATION_CYCLE_PLAN_DIR
) -> dict[str, Any]:
    plan_dir = output_dir / plan_id
    manifest = _read_optional_json(plan_dir / "confirmation_cycle_plan_manifest.json") or {}
    command_pack = _read_optional_json(plan_dir / "scheduled_command_pack.json") or {}
    snapshot = _read_optional_json(plan_dir / "confirmation_cycle_plan_input_snapshot.json") or {}
    recompute_error = ""
    expected_pack: dict[str, Any] = {}
    expected_manifest: dict[str, Any] = {}
    expected_runbook = ""
    expected_report = ""
    source_ok = False
    try:
        if snapshot.get("schema_version") != CONFIRMATION_CYCLE_PLAN_SNAPSHOT_SCHEMA_VERSION:
            raise DynamicV3ConfirmationOperationsError("plan snapshot schema mismatch")
        generated = _operations_datetime(snapshot.get("generated_at"), field="plan generated_at")
        config_path = Path(_text(snapshot.get("config_path")))
        if _operations_file_commitment(config_path) != _mapping(snapshot.get("config_commitment")):
            raise DynamicV3ConfirmationOperationsError("plan config commitment drift")
        validation = validate_confirmation_cycle_schedule_config(config_path=config_path)
        _strict_validation(validation, source_name="confirmation cycle config")
        if validation != _mapping(snapshot.get("config_validation")) or _mapping(
            validation.get("config")
        ) != _mapping(snapshot.get("config")):
            raise DynamicV3ConfirmationOperationsError("plan config snapshot drift")
        config = _mapping(snapshot.get("config"))
        expected_pack = {
            "schema_version": SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_scheduled_command_pack",
            "plan_id": plan_id,
            "cadence": _text(_mapping(config.get("schedule")).get("cadence"), "weekly"),
            "preferred_weekday": _text(_mapping(config.get("schedule")).get("preferred_weekday")),
            "timezone": _text(_mapping(config.get("schedule")).get("timezone")),
            "safety": _schedule_safety(config),
            "commands": _scheduled_commands(config_path),
            "production_effect": "none",
            "broker_action_allowed": False,
            "broker_action_taken": False,
        }
        expected_manifest = {
            "schema_version": SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_confirmation_cycle_plan_manifest",
            "plan_id": plan_id,
            "generated_at": generated.isoformat(),
            "status": "PASS",
            "config_path": str(config_path),
            "planned_step_count": len(expected_pack["commands"]),
            "confirmation_cycle_plan_manifest_path": str(
                plan_dir / "confirmation_cycle_plan_manifest.json"
            ),
            "scheduled_command_pack_path": str(plan_dir / "scheduled_command_pack.json"),
            "confirmation_cycle_plan_input_snapshot_path": str(
                plan_dir / "confirmation_cycle_plan_input_snapshot.json"
            ),
            "confirmation_cycle_runbook_path": str(plan_dir / "confirmation_cycle_runbook.md"),
            "confirmation_cycle_plan_report_path": str(
                plan_dir / "confirmation_cycle_plan_report.md"
            ),
            "market_regime": "ai_after_chatgpt",
            **_artifact_safety(),
        }
        expected_runbook = render_confirmation_cycle_runbook(expected_manifest, expected_pack)
        expected_report = render_confirmation_cycle_plan_report(expected_manifest, expected_pack)
        source_ok = True
    except Exception as exc:  # noqa: BLE001
        recompute_error = str(exc)
    checks = [
        _check(
            "manifest_exists", (plan_dir / "confirmation_cycle_plan_manifest.json").is_file(), ""
        ),
        _check("command_pack_exists", (plan_dir / "scheduled_command_pack.json").is_file(), ""),
        _check(
            "input_snapshot_exists",
            (plan_dir / "confirmation_cycle_plan_input_snapshot.json").is_file(),
            "",
        ),
        _check("runbook_exists", (plan_dir / "confirmation_cycle_runbook.md").is_file(), ""),
        _check("report_exists", (plan_dir / "confirmation_cycle_plan_report.md").is_file(), ""),
        _check("source_snapshot_valid", source_ok, recompute_error),
        _check("command_pack_recomputed", source_ok and command_pack == expected_pack, ""),
        _check("manifest_recomputed", source_ok and manifest == expected_manifest, ""),
        _check(
            "runbook_recomputed",
            source_ok
            and _read_text(plan_dir / "confirmation_cycle_runbook.md") == expected_runbook,
            "",
        ),
        _check(
            "report_recomputed",
            source_ok
            and _read_text(plan_dir / "confirmation_cycle_plan_report.md") == expected_report,
            "",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_confirmation_cycle_plan_validation",
        artifact_id_key="plan_id",
        artifact_id=plan_id,
        checks=checks,
    )


def _weekly_validation(*, kind: str, artifact_id: str, root: Path) -> dict[str, Any]:
    validators = {
        "outcome_due": lambda: validate_outcome_due_artifact(due_id=artifact_id, output_dir=root),
        "outcome_update_review": lambda: validate_outcome_update_review_artifact(
            review_id=artifact_id, output_dir=root
        ),
        "outcome_update": lambda: validate_outcome_update_artifact(
            update_id=artifact_id, output_dir=root
        ),
        "rolling_refresh": lambda: validate_rolling_evidence_refresh_artifact(
            refresh_id=artifact_id, output_dir=root
        ),
        "evidence_trend": lambda: validate_evidence_trend_artifact(
            trend_id=artifact_id, output_dir=root
        ),
        "confirmation_progress": lambda: validate_confirmation_progress_artifact(
            progress_id=artifact_id, output_dir=root
        ),
        "confirmation_evaluation": lambda: validate_confirmation_evaluation_artifact(
            evaluation_id=artifact_id, output_dir=root
        ),
        "rule_review_cycle": lambda: validate_rule_review_cycle_artifact(
            cycle_id=artifact_id, output_dir=root
        ),
        "rule_review_queue": lambda: validate_rule_review_queue_artifact(
            queue_id=artifact_id, output_dir=root
        ),
        "forward_outcome_decision": lambda: validate_forward_outcome_decision_artifact(
            decision_id=artifact_id, output_dir=root
        ),
        "confirmation_dashboard": lambda: validate_confirmation_dashboard_artifact(
            dashboard_id=artifact_id, output_dir=root
        ),
    }
    validator = validators.get(kind)
    if validator is None:
        raise DynamicV3ConfirmationOperationsError(f"unknown weekly source kind: {kind}")
    return _strict_validation(validator(), source_name=kind)


def _weekly_source_record(
    *, kind: str, artifact_id: str, root: Path, generated: datetime
) -> dict[str, Any]:
    validation = _weekly_validation(kind=kind, artifact_id=artifact_id, root=root)
    artifact_dir = root / artifact_id
    names = sorted(path.name for path in artifact_dir.iterdir() if path.is_file())
    json_views = [
        name
        for name in names
        if name.endswith(".json") and "snapshot" not in name and "transaction" not in name
    ]
    jsonl_views = [name for name in names if name.endswith(".jsonl")]
    bundle = _operations_source_bundle(
        source_dir=artifact_dir,
        canonical_files=names,
        json_views=json_views,
        jsonl_views=jsonl_views,
    )
    manifests = [
        _mapping(value)
        for name, value in _mapping(bundle.get("json")).items()
        if "manifest" in name
    ]
    if len(manifests) != 1:
        raise DynamicV3ConfirmationOperationsError(
            f"{kind} must expose exactly one top-level manifest"
        )
    source_generated = _source_not_after_cutoff(manifests[0], generated=generated, source_name=kind)
    return {
        "kind": kind,
        "artifact_id": artifact_id,
        "root": str(root),
        "generated_at": source_generated.isoformat(),
        "validation": validation,
        "bundle": bundle,
    }


def _weekly_snapshot_source(snapshot: Mapping[str, Any], kind: str) -> dict[str, Any] | None:
    matches = [row for row in _records(snapshot.get("sources")) if row.get("kind") == kind]
    if len(matches) > 1:
        raise DynamicV3ConfirmationOperationsError(f"duplicate weekly source kind: {kind}")
    return matches[0] if matches else None


def _weekly_source_json(source: Mapping[str, Any], name: str) -> dict[str, Any]:
    payload = _mapping(_mapping(source.get("bundle")).get("json"))
    value = payload.get(name)
    if not isinstance(value, Mapping):
        raise DynamicV3ConfirmationOperationsError(
            f"weekly source view missing: {source.get('kind')}:{name}"
        )
    return dict(value)


def _weekly_summary_from_snapshot(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    due = _weekly_snapshot_source(snapshot, "outcome_due")
    progress = _weekly_snapshot_source(snapshot, "confirmation_progress")
    evaluation = _weekly_snapshot_source(snapshot, "confirmation_evaluation")
    cycle = _weekly_snapshot_source(snapshot, "rule_review_cycle")
    queue = _weekly_snapshot_source(snapshot, "rule_review_queue")
    if not all((due, progress, evaluation, cycle, queue)):
        raise DynamicV3ConfirmationOperationsError("weekly core source set incomplete")
    update = _weekly_snapshot_source(snapshot, "outcome_update")
    update_payload: dict[str, Any] | None = None
    if update is not None:
        update_manifest = next(
            (
                _mapping(value)
                for name, value in _mapping(_mapping(update.get("bundle")).get("json")).items()
                if "manifest" in name
            ),
            {},
        )
        updated_rows = next(
            (
                _records(value)
                for name, value in _mapping(_mapping(update.get("bundle")).get("jsonl")).items()
                if "updated" in name
            ),
            [],
        )
        skipped_rows = next(
            (
                _records(value)
                for name, value in _mapping(_mapping(update.get("bundle")).get("jsonl")).items()
                if "skipped" in name
            ),
            [],
        )
        update_payload = {
            "manifest": update_manifest,
            "updated_windows": updated_rows,
            "skipped_windows": skipped_rows,
        }
    summary = _weekly_cycle_summary(
        week_ending=date.fromisoformat(_text(snapshot.get("week_ending"))),
        due_summary=_weekly_source_json(due, "pending_window_summary.json"),
        update=update_payload,
        progress_summary=_weekly_source_json(progress, "target_progress_summary.json"),
        evaluation_summary=_weekly_source_json(evaluation, "confirmation_evaluation_summary.json"),
        cycle_manifest=_weekly_source_json(cycle, "rule_review_cycle_manifest.json"),
        queue_summary=_weekly_source_json(queue, "queue_summary.json"),
    )
    summary["weekly_cycle_id"] = _text(snapshot.get("weekly_cycle_id"))
    return summary


@with_artifact_validation_session
def run_confirmation_cycle_weekly(
    *,
    week_ending: date,
    config_path: Path = DEFAULT_CONFIRMATION_CYCLE_SCHEDULE_CONFIG_PATH,
    execute_ready_updates: bool = False,
    registry_id: str | None = None,
    output_dir: Path = DEFAULT_CONFIRMATION_CYCLE_WEEKLY_DIR,
    outcome_due_dir: Path = DEFAULT_OUTCOME_DUE_DIR,
    outcome_update_review_dir: Path = DEFAULT_OUTCOME_UPDATE_REVIEW_DIR,
    outcome_update_dir: Path = DEFAULT_OUTCOME_UPDATE_DIR,
    rolling_refresh_dir: Path = DEFAULT_ROLLING_EVIDENCE_REFRESH_DIR,
    evidence_trend_dir: Path = DEFAULT_EVIDENCE_TREND_DIR,
    forward_decision_dir: Path = DEFAULT_FORWARD_OUTCOME_DECISION_DIR,
    registry_dir: Path = DEFAULT_CONFIRMATION_REGISTRY_DIR,
    progress_dir: Path = DEFAULT_CONFIRMATION_PROGRESS_DIR,
    evaluation_dir: Path = DEFAULT_CONFIRMATION_EVALUATION_DIR,
    rule_cycle_dir: Path = DEFAULT_RULE_REVIEW_CYCLE_DIR,
    queue_dir: Path = DEFAULT_RULE_REVIEW_QUEUE_DIR,
    rule_owner_decision_journal_path: Path = DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH,
    dashboard_dir: Path = DEFAULT_CONFIRMATION_DASHBOARD_DIR,
    pressure_tag_dir: Path = DEFAULT_PRESSURE_REGIME_TAG_DIR,
    advisory_outcome_dir: Path = DEFAULT_ADVISORY_OUTCOME_DIR,
    limited_vs_notrade_dir: Path = DEFAULT_LIMITED_VS_NOTRADE_DIR,
    consensus_risk_dir: Path = DEFAULT_CONSENSUS_RISK_DIR,
    daily_advisory_dir: Path = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    owner_review_dir: Path = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    shadow_shortlist_dir: Path = DEFAULT_SHADOW_SHORTLIST_DIR,
    shadow_monitor_run_dir: Path = DEFAULT_SHADOW_MONITOR_RUN_DIR,
    consensus_drift_dir: Path = DEFAULT_CONSENSUS_DRIFT_DIR,
    paper_portfolio_config_path: Path = DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    enforce_data_quality_gate: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _operations_generated_at(generated_at)
    if week_ending > generated.date():
        raise DynamicV3ConfirmationOperationsError("week_ending must not exceed generated cutoff")
    validation = validate_confirmation_cycle_schedule_config(config_path=config_path)
    _strict_validation(validation, source_name="confirmation cycle config")
    config_commitment = _operations_file_commitment(config_path)
    resolved_registry_id = _semantic_artifact_id(
        output_dir=registry_dir,
        artifact_id=registry_id,
        manifest_name="confirmation_registry_manifest.json",
        id_key="registry_id",
        generated=generated,
        source_name="confirmation registry",
        required=True,
    )
    registry_validation = _strict_validation(
        validate_confirmation_targets_artifact(
            registry_id=resolved_registry_id, output_dir=registry_dir
        ),
        source_name="confirmation registry",
    )
    registry_manifest = _read_json(
        registry_dir / resolved_registry_id / "confirmation_registry_manifest.json"
    )
    _source_not_after_cutoff(
        registry_manifest, generated=generated, source_name="confirmation registry"
    )
    weekly_cycle_id = _stable_id(
        "confirmation-cycle-weekly",
        week_ending.isoformat(),
        generated.isoformat(),
    )
    weekly_dir = output_dir / weekly_cycle_id
    if weekly_dir.exists():
        raise DynamicV3ConfirmationOperationsError(
            f"weekly cycle already exists: {weekly_cycle_id}"
        )
    steps: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    artifacts: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_cycle_artifacts",
    }
    due = run_outcome_due_scan(
        as_of=week_ending,
        output_dir=outcome_due_dir,
        advisory_outcome_dir=advisory_outcome_dir,
        prices_path=prices_path,
        rates_path=rates_path,
        enforce_data_quality_gate=enforce_data_quality_gate,
        generated_at=generated,
    )
    due_summary = _mapping(due.get("pending_window_summary"))
    artifacts["outcome_due_id"] = due["due_id"]
    sources.append(
        _weekly_source_record(
            kind="outcome_due",
            artifact_id=due["due_id"],
            root=outcome_due_dir,
            generated=generated,
        )
    )
    steps.append(
        _step(
            "outcome_due_scan",
            "PASS",
            due["due_id"],
            due_windows=_int(due_summary.get("due_windows")),
            update_ready_count=_int(due_summary.get("update_ready_count")),
        )
    )
    review = run_outcome_update_review(
        due_id=due["due_id"],
        output_dir=outcome_update_review_dir,
        outcome_due_dir=outcome_due_dir,
        generated_at=generated,
    )
    review_manifest = _mapping(review.get("manifest"))
    artifacts["outcome_update_review_id"] = review["update_review_id"]
    sources.append(
        _weekly_source_record(
            kind="outcome_update_review",
            artifact_id=review["update_review_id"],
            root=outcome_update_review_dir,
            generated=generated,
        )
    )
    steps.append(
        _step(
            "outcome_update_review",
            _text(review_manifest.get("status"), "PASS"),
            review["update_review_id"],
            ready_to_update_count=_int(review_manifest.get("ready_to_update_count")),
            blocked_count=_int(review_manifest.get("blocked_count")),
        )
    )
    update: dict[str, Any] | None = None
    if execute_ready_updates and _int(review_manifest.get("ready_to_update_count")) > 0:
        update = run_outcome_update(
            update_review_id=review["update_review_id"],
            output_dir=outcome_update_dir,
            review_dir=outcome_update_review_dir,
            advisory_outcome_dir=advisory_outcome_dir,
            prices_path=prices_path,
            rates_path=rates_path,
            generated_at=generated,
        )
        artifacts["outcome_update_id"] = update["outcome_update_id"]
        sources.append(
            _weekly_source_record(
                kind="outcome_update",
                artifact_id=update["outcome_update_id"],
                root=outcome_update_dir,
                generated=generated,
            )
        )
        steps.append(
            _step(
                "outcome_update",
                _text(_mapping(update.get("manifest")).get("status"), "PASS"),
                update["outcome_update_id"],
                updated_windows=len(update["updated_windows"]),
                skipped_windows=len(update["skipped_windows"]),
            )
        )
    else:
        reason = "execute_ready_updates_false" if not execute_ready_updates else "no_ready_updates"
        artifacts["outcome_update_id"] = ""
        steps.append(_step("outcome_update", "SKIPPED", "", reason=reason))
    refresh: dict[str, Any] | None = None
    if update is not None:
        refresh = run_rolling_evidence_refresh(
            outcome_update_id=update["outcome_update_id"],
            output_dir=rolling_refresh_dir,
            outcome_update_dir=outcome_update_dir,
            advisory_outcome_dir=advisory_outcome_dir,
            daily_advisory_dir=daily_advisory_dir,
            owner_review_dir=owner_review_dir,
            shadow_shortlist_dir=shadow_shortlist_dir,
            shadow_monitor_run_dir=shadow_monitor_run_dir,
            consensus_drift_dir=consensus_drift_dir,
            config_path=paper_portfolio_config_path,
            outcome_due_dir=outcome_due_dir,
            limited_vs_notrade_dir=limited_vs_notrade_dir,
            consensus_risk_dir=consensus_risk_dir,
            generated_at=generated,
        )
        artifacts["rolling_refresh_id"] = refresh["refresh_id"]
        sources.append(
            _weekly_source_record(
                kind="rolling_refresh",
                artifact_id=refresh["refresh_id"],
                root=rolling_refresh_dir,
                generated=generated,
            )
        )
        steps.append(
            _step(
                "rolling_evidence_refresh",
                _text(_mapping(refresh.get("manifest")).get("status"), "PASS"),
                refresh["refresh_id"],
            )
        )
    else:
        artifacts["rolling_refresh_id"] = ""
        steps.append(
            _step(
                "rolling_evidence_refresh",
                "SKIPPED",
                "",
                reason="no_outcome_update_executed",
            )
        )
    trend = run_evidence_trend(
        output_dir=evidence_trend_dir,
        rolling_refresh_dir=rolling_refresh_dir,
        generated_at=generated,
    )
    artifacts["evidence_trend_id"] = trend["trend_id"]
    sources.append(
        _weekly_source_record(
            kind="evidence_trend",
            artifact_id=trend["trend_id"],
            root=evidence_trend_dir,
            generated=generated,
        )
    )
    steps.append(
        _step(
            "evidence_trend",
            _text(_mapping(trend.get("manifest")).get("status"), "PASS"),
            trend["trend_id"],
            trend_status=_text(_mapping(trend.get("confidence_trend_summary")).get("trend_status")),
        )
    )
    progress = update_confirmation_progress(
        registry_id=resolved_registry_id,
        registry_dir=registry_dir,
        output_dir=progress_dir,
        limited_vs_notrade_dir=limited_vs_notrade_dir,
        consensus_risk_dir=consensus_risk_dir,
        generated_at=generated,
    )
    progress_summary = _mapping(progress.get("target_progress_summary"))
    artifacts["confirmation_progress_id"] = progress["progress_id"]
    sources.append(
        _weekly_source_record(
            kind="confirmation_progress",
            artifact_id=progress["progress_id"],
            root=progress_dir,
            generated=generated,
        )
    )
    steps.append(
        _step(
            "confirmation_progress",
            _text(_mapping(progress.get("manifest")).get("status"), "PASS"),
            progress["progress_id"],
            ready_for_evaluation_count=_int(progress_summary.get("ready_for_evaluation_count")),
            insufficient_events_count=_int(progress_summary.get("insufficient_events_count")),
        )
    )
    evaluation = run_confirmation_evaluation(
        progress_id=progress["progress_id"],
        progress_dir=progress_dir,
        output_dir=evaluation_dir,
        generated_at=generated,
    )
    evaluation_summary = _mapping(evaluation.get("confirmation_evaluation_summary"))
    artifacts["confirmation_evaluation_id"] = evaluation["evaluation_id"]
    sources.append(
        _weekly_source_record(
            kind="confirmation_evaluation",
            artifact_id=evaluation["evaluation_id"],
            root=evaluation_dir,
            generated=generated,
        )
    )
    steps.append(
        _step(
            "confirmation_evaluate",
            _text(_mapping(evaluation.get("manifest")).get("status"), "PASS"),
            evaluation["evaluation_id"],
            success_count=_int(evaluation_summary.get("success_count")),
            failure_count=_int(evaluation_summary.get("failure_count")),
            not_ready_count=_int(evaluation_summary.get("not_ready_count")),
        )
    )
    rule_cycle = run_rule_review_cycle(
        registry_id=resolved_registry_id,
        progress_id=progress["progress_id"],
        evaluation_id=evaluation["evaluation_id"],
        registry_dir=registry_dir,
        progress_dir=progress_dir,
        evaluation_dir=evaluation_dir,
        output_dir=rule_cycle_dir,
        generated_at=generated,
    )
    artifacts["rule_review_cycle_id"] = rule_cycle["cycle_id"]
    cycle_manifest = _mapping(rule_cycle.get("manifest"))
    sources.append(
        _weekly_source_record(
            kind="rule_review_cycle",
            artifact_id=rule_cycle["cycle_id"],
            root=rule_cycle_dir,
            generated=generated,
        )
    )
    steps.append(
        _step(
            "rule_review_cycle",
            _text(cycle_manifest.get("status"), "PASS"),
            rule_cycle["cycle_id"],
            cycle_recommendation=_text(cycle_manifest.get("cycle_recommendation")),
            owner_action_count=_int(cycle_manifest.get("targets_requiring_owner_action")),
        )
    )
    queue = build_rule_review_queue(
        cycle_id=rule_cycle["cycle_id"],
        output_dir=queue_dir,
        cycle_dir=rule_cycle_dir,
        journal_path=rule_owner_decision_journal_path,
        generated_at=generated,
    )
    artifacts["rule_review_queue_id"] = queue["queue_id"]
    sources.append(
        _weekly_source_record(
            kind="rule_review_queue",
            artifact_id=queue["queue_id"],
            root=queue_dir,
            generated=generated,
        )
    )
    steps.append(
        _step(
            "owner_decision_queue_update",
            _text(_mapping(queue.get("manifest")).get("status"), "PASS"),
            queue["queue_id"],
            ready_for_owner_review_count=_int(
                _mapping(queue.get("queue_summary")).get("ready_for_owner_review_count")
            ),
        )
    )
    if update is not None and refresh is not None:
        forward_decision = run_forward_outcome_decision(
            week_ending=week_ending,
            output_dir=forward_decision_dir,
            outcome_update_dir=outcome_update_dir,
            rolling_refresh_dir=rolling_refresh_dir,
            evidence_trend_dir=evidence_trend_dir,
            outcome_update_id=update["outcome_update_id"],
            refresh_id=refresh["refresh_id"],
            trend_id=trend["trend_id"],
            generated_at=generated,
        )
        artifacts["forward_outcome_decision_id"] = forward_decision["decision_id"]
        sources.append(
            _weekly_source_record(
                kind="forward_outcome_decision",
                artifact_id=forward_decision["decision_id"],
                root=forward_decision_dir,
                generated=generated,
            )
        )
        steps.append(
            _step(
                "forward_outcome_decision",
                _text(_mapping(forward_decision.get("manifest")).get("status"), "PASS"),
                forward_decision["decision_id"],
            )
        )
    else:
        artifacts["forward_outcome_decision_id"] = ""
        steps.append(
            _step(
                "forward_outcome_decision",
                "SKIPPED",
                "",
                reason="no_current_outcome_update_or_refresh_artifact",
            )
        )
    dashboard = build_confirmation_dashboard(
        week_ending=week_ending,
        weekly_cycle_id="",
        weekly_cycle_reference_id=weekly_cycle_id,
        progress_id=progress["progress_id"],
        evaluation_id=evaluation["evaluation_id"],
        cycle_id=rule_cycle["cycle_id"],
        queue_id=queue["queue_id"],
        output_dir=dashboard_dir,
        weekly_cycle_dir=output_dir,
        progress_dir=progress_dir,
        evaluation_dir=evaluation_dir,
        rule_cycle_dir=rule_cycle_dir,
        queue_dir=queue_dir,
        pressure_tag_dir=pressure_tag_dir,
        generated_at=generated,
    )
    artifacts["confirmation_dashboard_id"] = dashboard["dashboard_id"]
    sources.append(
        _weekly_source_record(
            kind="confirmation_dashboard",
            artifact_id=dashboard["dashboard_id"],
            root=dashboard_dir,
            generated=generated,
        )
    )
    steps.append(
        _step(
            "weekly_dashboard",
            _text(_mapping(dashboard.get("manifest")).get("status"), "PASS"),
            dashboard["dashboard_id"],
            ready_for_evaluation_count=_int(
                _mapping(dashboard.get("confirmation_dashboard_summary")).get(
                    "ready_for_evaluation"
                )
            ),
        )
    )
    steps.append(_step("reader_brief_update", "PASS", dashboard["dashboard_id"]))
    summary = _weekly_cycle_summary(
        week_ending=week_ending,
        due_summary=due_summary,
        update=update,
        progress_summary=progress_summary,
        evaluation_summary=evaluation_summary,
        cycle_manifest=cycle_manifest,
        queue_summary=_mapping(queue.get("queue_summary")),
    )
    artifacts["weekly_cycle_id"] = weekly_dir.name
    summary["weekly_cycle_id"] = weekly_dir.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_cycle_manifest",
        "weekly_cycle_id": weekly_dir.name,
        "week_ending": week_ending.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "execute_ready_updates": execute_ready_updates,
        "dry_run": not execute_ready_updates,
        "weekly_cycle_manifest_path": str(weekly_dir / "weekly_cycle_manifest.json"),
        "weekly_cycle_steps_path": str(weekly_dir / "weekly_cycle_steps.json"),
        "weekly_cycle_artifacts_path": str(weekly_dir / "weekly_cycle_artifacts.json"),
        "weekly_cycle_summary_path": str(weekly_dir / "weekly_cycle_summary.json"),
        "confirmation_cycle_weekly_input_snapshot_path": str(
            weekly_dir / "confirmation_cycle_weekly_input_snapshot.json"
        ),
        "weekly_cycle_report_path": str(weekly_dir / "weekly_cycle_report.md"),
        "reader_brief_section_path": str(weekly_dir / "reader_brief_section.md"),
        **_artifact_safety(),
    }
    snapshot = {
        "schema_version": CONFIRMATION_CYCLE_WEEKLY_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_cycle_weekly_input_snapshot",
        "weekly_cycle_id": weekly_cycle_id,
        "week_ending": week_ending.isoformat(),
        "generated_at": generated.isoformat(),
        "execute_ready_updates": execute_ready_updates,
        "config_path": str(config_path),
        "config_commitment": config_commitment,
        "config": _mapping(validation.get("config")),
        "config_validation": validation,
        "registry_id": resolved_registry_id,
        "registry_root": str(registry_dir),
        "registry_validation": registry_validation,
        "registry_bundle": _operations_source_bundle(
            source_dir=registry_dir / resolved_registry_id,
            json_views=("confirmation_registry_manifest.json",),
        ),
        "sources": sources,
        "materialized_views": {
            "steps": steps,
            "artifacts": artifacts,
            "summary": summary,
        },
        "production_effect": "none",
    }
    report = render_weekly_cycle_report(manifest, steps, summary)
    reader_brief = render_weekly_cycle_reader_brief(summary)
    weekly_dir.mkdir(parents=True, exist_ok=False)
    write_json_atomic(weekly_dir / "weekly_cycle_manifest.json", manifest)
    write_json_atomic(
        weekly_dir / "weekly_cycle_steps.json",
        {"schema_version": SCHEMA_VERSION, "steps": steps},
    )
    write_json_atomic(weekly_dir / "weekly_cycle_artifacts.json", artifacts)
    write_json_atomic(weekly_dir / "weekly_cycle_summary.json", summary)
    write_json_atomic(weekly_dir / "confirmation_cycle_weekly_input_snapshot.json", snapshot)
    write_text_atomic(
        weekly_dir / "weekly_cycle_report.md",
        report,
    )
    write_text_atomic(
        weekly_dir / "reader_brief_section.md",
        reader_brief,
    )
    _update_latest_pointer(
        "latest_confirmation_cycle_weekly",
        weekly_dir.name,
        weekly_dir / "weekly_cycle_manifest.json",
    )
    return {
        "weekly_cycle_id": weekly_dir.name,
        "weekly_cycle_dir": weekly_dir,
        "manifest": manifest,
        "weekly_cycle_steps": {"steps": steps},
        "weekly_cycle_artifacts": artifacts,
        "weekly_cycle_summary": summary,
        "input_snapshot": snapshot,
    }


def confirmation_cycle_weekly_report_payload(
    *,
    weekly_cycle_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CONFIRMATION_CYCLE_WEEKLY_DIR,
) -> dict[str, Any]:
    weekly_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=weekly_cycle_id if not latest else None,
        pointer_name="latest_confirmation_cycle_weekly",
    )
    return {
        **_read_json(weekly_dir / "weekly_cycle_manifest.json"),
        "weekly_cycle_steps": _read_json(weekly_dir / "weekly_cycle_steps.json"),
        "weekly_cycle_artifacts": _read_json(weekly_dir / "weekly_cycle_artifacts.json"),
        "weekly_cycle_summary": _read_json(weekly_dir / "weekly_cycle_summary.json"),
        **_report_input_snapshot(weekly_dir / "confirmation_cycle_weekly_input_snapshot.json"),
        "reader_brief_section": _read_text(weekly_dir / "reader_brief_section.md"),
        "weekly_cycle_dir": str(weekly_dir),
    }


@with_artifact_validation_session
def validate_confirmation_cycle_weekly_artifact(
    *, weekly_cycle_id: str, output_dir: Path = DEFAULT_CONFIRMATION_CYCLE_WEEKLY_DIR
) -> dict[str, Any]:
    weekly_dir = output_dir / weekly_cycle_id
    manifest = _read_optional_json(weekly_dir / "weekly_cycle_manifest.json") or {}
    steps_payload = _read_optional_json(weekly_dir / "weekly_cycle_steps.json") or {}
    steps = _records(steps_payload.get("steps"))
    summary = _read_optional_json(weekly_dir / "weekly_cycle_summary.json") or {}
    artifacts = _read_optional_json(weekly_dir / "weekly_cycle_artifacts.json") or {}
    snapshot = (
        _read_optional_json(weekly_dir / "confirmation_cycle_weekly_input_snapshot.json") or {}
    )
    step_names = {_text(row.get("step")) for row in steps}
    source_errors: list[str] = []
    expected_summary: dict[str, Any] = {}
    expected_manifest: dict[str, Any] = {}
    recompute_error = ""
    source_bindings_ok = False
    try:
        if snapshot.get("schema_version") != CONFIRMATION_CYCLE_WEEKLY_SNAPSHOT_SCHEMA_VERSION:
            raise DynamicV3ConfirmationOperationsError("weekly snapshot schema mismatch")
        generated = _operations_datetime(snapshot.get("generated_at"), field="weekly generated_at")
        if snapshot.get("weekly_cycle_id") != weekly_cycle_id:
            raise DynamicV3ConfirmationOperationsError("weekly snapshot id mismatch")
        config_path = Path(_text(snapshot.get("config_path")))
        if _operations_file_commitment(config_path) != _mapping(snapshot.get("config_commitment")):
            raise DynamicV3ConfirmationOperationsError("weekly config commitment drift")
        config_validation = validate_confirmation_cycle_schedule_config(config_path=config_path)
        _strict_validation(config_validation, source_name="confirmation cycle config")
        if config_validation != _mapping(snapshot.get("config_validation")):
            raise DynamicV3ConfirmationOperationsError("weekly config validation drift")
        registry_id = _text(snapshot.get("registry_id"))
        registry_root = Path(_text(snapshot.get("registry_root")))
        registry_validation = _strict_validation(
            validate_confirmation_targets_artifact(
                registry_id=registry_id, output_dir=registry_root
            ),
            source_name="confirmation registry",
        )
        if registry_validation != _mapping(snapshot.get("registry_validation")):
            raise DynamicV3ConfirmationOperationsError("weekly registry validation drift")
        source_errors.extend(
            _validate_operations_source_bundle(_mapping(snapshot.get("registry_bundle")))
        )
        source_ids: dict[str, str] = {}
        for source in _records(snapshot.get("sources")):
            kind = _text(source.get("kind"))
            artifact_id = _text(source.get("artifact_id"))
            root = Path(_text(source.get("root")))
            if not kind or not artifact_id or kind in source_ids:
                source_errors.append("weekly source identity missing or duplicate")
                continue
            live_validation = _weekly_validation(kind=kind, artifact_id=artifact_id, root=root)
            if live_validation != _mapping(source.get("validation")):
                source_errors.append(f"weekly source validation drift: {kind}")
            source_errors.extend(_validate_operations_source_bundle(_mapping(source.get("bundle"))))
            source_generated = _operations_datetime(
                source.get("generated_at"), field=f"{kind} generated_at"
            )
            if source_generated > generated:
                source_errors.append(f"weekly source after cutoff: {kind}")
            source_ids[kind] = artifact_id
        artifact_keys = {
            "outcome_due": "outcome_due_id",
            "outcome_update_review": "outcome_update_review_id",
            "outcome_update": "outcome_update_id",
            "rolling_refresh": "rolling_refresh_id",
            "evidence_trend": "evidence_trend_id",
            "confirmation_progress": "confirmation_progress_id",
            "confirmation_evaluation": "confirmation_evaluation_id",
            "rule_review_cycle": "rule_review_cycle_id",
            "rule_review_queue": "rule_review_queue_id",
            "forward_outcome_decision": "forward_outcome_decision_id",
            "confirmation_dashboard": "confirmation_dashboard_id",
        }
        source_bindings_ok = all(
            artifacts.get(key) == source_ids.get(kind, "") for kind, key in artifact_keys.items()
        )
        expected_summary = _weekly_summary_from_snapshot(snapshot)
        expected_manifest = {
            "schema_version": SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_weekly_cycle_manifest",
            "weekly_cycle_id": weekly_cycle_id,
            "week_ending": snapshot.get("week_ending"),
            "generated_at": generated.isoformat(),
            "status": "PASS",
            "execute_ready_updates": snapshot.get("execute_ready_updates") is True,
            "dry_run": snapshot.get("execute_ready_updates") is not True,
            "weekly_cycle_manifest_path": str(weekly_dir / "weekly_cycle_manifest.json"),
            "weekly_cycle_steps_path": str(weekly_dir / "weekly_cycle_steps.json"),
            "weekly_cycle_artifacts_path": str(weekly_dir / "weekly_cycle_artifacts.json"),
            "weekly_cycle_summary_path": str(weekly_dir / "weekly_cycle_summary.json"),
            "confirmation_cycle_weekly_input_snapshot_path": str(
                weekly_dir / "confirmation_cycle_weekly_input_snapshot.json"
            ),
            "weekly_cycle_report_path": str(weekly_dir / "weekly_cycle_report.md"),
            "reader_brief_section_path": str(weekly_dir / "reader_brief_section.md"),
            **_artifact_safety(),
        }
    except Exception as exc:  # noqa: BLE001
        recompute_error = str(exc)
    materialized = _mapping(snapshot.get("materialized_views"))
    expected_steps = _records(materialized.get("steps"))
    expected_artifacts = _mapping(materialized.get("artifacts"))
    checks = [
        _check("manifest_exists", (weekly_dir / "weekly_cycle_manifest.json").exists(), ""),
        _check("steps_exists", (weekly_dir / "weekly_cycle_steps.json").exists(), ""),
        _check("artifacts_exists", (weekly_dir / "weekly_cycle_artifacts.json").exists(), ""),
        _check("summary_exists", (weekly_dir / "weekly_cycle_summary.json").exists(), ""),
        _check("report_exists", (weekly_dir / "weekly_cycle_report.md").exists(), ""),
        _check("reader_brief_exists", (weekly_dir / "reader_brief_section.md").exists(), ""),
        _check(
            "input_snapshot_exists",
            (weekly_dir / "confirmation_cycle_weekly_input_snapshot.json").exists(),
            "",
        ),
        _check("weekly_cycle_id_matches", manifest.get("weekly_cycle_id") == weekly_cycle_id, ""),
        _check(
            "required_steps_present",
            {"outcome_due_scan", "confirmation_progress", "confirmation_evaluate"} <= step_names,
            "core weekly steps",
        ),
        _check(
            "dry_run_blocks_default_update",
            manifest.get("execute_ready_updates") is True
            or any(
                row.get("step") == "outcome_update" and row.get("status") == "SKIPPED"
                for row in steps
            ),
            "default update must be skipped",
        ),
        _check("summary_id_matches", summary.get("weekly_cycle_id") == weekly_cycle_id, ""),
        _check("artifact_id_matches", artifacts.get("weekly_cycle_id") == weekly_cycle_id, ""),
        _check("source_recompute_succeeded", not recompute_error, recompute_error),
        _check("source_bundles_valid", not source_errors, "; ".join(source_errors)),
        _check("source_artifact_bindings", source_bindings_ok, "source ids must match artifacts"),
        _check("steps_recomputed", steps == expected_steps, "snapshot materialized steps"),
        _check("artifacts_recomputed", artifacts == expected_artifacts, "snapshot artifacts"),
        _check("summary_recomputed", summary == expected_summary, "source-derived summary"),
        _check("manifest_recomputed", manifest == expected_manifest, "manifest contract"),
        _check(
            "report_recomputed",
            not recompute_error
            and _read_text(weekly_dir / "weekly_cycle_report.md")
            == render_weekly_cycle_report(expected_manifest, expected_steps, expected_summary),
            "",
        ),
        _check(
            "reader_brief_recomputed",
            not recompute_error
            and _read_text(weekly_dir / "reader_brief_section.md")
            == render_weekly_cycle_reader_brief(expected_summary),
            "",
        ),
        _check(
            "safety_no_broker",
            manifest.get("broker_action_allowed") is False
            and summary.get("broker_action_allowed") is False,
            "broker disabled",
        ),
        _check(
            "production_effect_none",
            manifest.get("production_effect") == "none"
            and summary.get("production_effect") == "none",
            "production none",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_confirmation_cycle_weekly_validation",
        artifact_id_key="weekly_cycle_id",
        artifact_id=weekly_cycle_id,
        checks=checks,
    )


def _pressure_outcome_sources(
    *, advisory_outcome_dir: Path, start: date, end: date, generated: datetime
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    selected: list[dict[str, Any]] = []
    inventory: list[dict[str, Any]] = []
    seen_daily: set[str] = set()
    for manifest_path in sorted(advisory_outcome_dir.glob("*/advisory_outcome_manifest.json")):
        manifest = _read_optional_json(manifest_path)
        if not manifest:
            raise DynamicV3ConfirmationOperationsError(
                f"advisory outcome manifest invalid: {manifest_path}"
            )
        outcome_id = _text(manifest.get("outcome_id"))
        if not outcome_id or outcome_id != manifest_path.parent.name:
            raise DynamicV3ConfirmationOperationsError(
                f"advisory outcome identity invalid: {manifest_path}"
            )
        outcome_generated = _operations_datetime(
            manifest.get("generated_at"), field="advisory outcome generated_at"
        )
        windows = _read_jsonl(manifest_path.parent / "outcome_windows.jsonl")
        relevant_windows = [
            row
            for row in windows
            if (
                (parsed := _date_or_none(row.get("end_date"))) is not None
                and start <= parsed <= end
            )
        ]
        inventory.append(
            {
                "outcome_id": outcome_id,
                "generated_at": outcome_generated.isoformat(),
                "selected": outcome_generated <= generated and bool(relevant_windows),
                "relevant_window_count": len(relevant_windows),
                "manifest_commitment": _operations_file_commitment(manifest_path),
            }
        )
        if outcome_generated > generated or not relevant_windows:
            continue
        validation = _strict_validation(
            validate_advisory_outcome_artifact(
                outcome_id=outcome_id, output_dir=advisory_outcome_dir
            ),
            source_name=f"advisory outcome {outcome_id}",
        )
        updated_at = manifest.get("updated_at")
        if (
            updated_at
            and _operations_datetime(updated_at, field="advisory outcome updated_at") > generated
        ):
            raise DynamicV3ConfirmationOperationsError(
                f"advisory outcome updated after cutoff: {outcome_id}"
            )
        daily_id = _text(manifest.get("daily_advisory_id"))
        if not daily_id or daily_id in seen_daily:
            raise DynamicV3ConfirmationOperationsError(
                f"advisory outcome daily id missing or duplicate: {daily_id}"
            )
        seen_daily.add(daily_id)
        names = sorted(path.name for path in manifest_path.parent.iterdir() if path.is_file())
        selected.append(
            {
                "outcome_id": outcome_id,
                "daily_advisory_id": daily_id,
                "validation": validation,
                "bundle": _operations_source_bundle(
                    source_dir=manifest_path.parent,
                    canonical_files=names,
                    json_views=(
                        "advisory_outcome_manifest.json",
                        "advisory_event.json",
                        "advisory_counterfactuals.json",
                    ),
                    jsonl_views=("outcome_windows.jsonl", "outcome_update_events.jsonl"),
                ),
            }
        )
    return selected, inventory


def _date_or_none(value: Any) -> date | None:
    try:
        return date.fromisoformat(_text(value)[:10])
    except ValueError:
        return None


def _build_outcome_regime_tags_from_sources(
    *,
    sources: Sequence[Mapping[str, Any]],
    window_tags: Sequence[Mapping[str, Any]],
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    tags_by_end: dict[tuple[str, int], list[str]] = {
        (_text(row.get("end_date")), _int(row.get("window_days"))): _records_to_texts(
            row.get("regime_tags")
        )
        for row in window_tags
    }
    rows: list[dict[str, Any]] = []
    for source in sources:
        bundle = _mapping(source.get("bundle"))
        json_views = _mapping(bundle.get("json"))
        jsonl_views = _mapping(bundle.get("jsonl"))
        manifest = _mapping(json_views.get("advisory_outcome_manifest.json"))
        windows = _records(jsonl_views.get("outcome_windows.jsonl"))
        for window in windows:
            window_end = _date_or_none(window.get("end_date"))
            if window_end is None or not start <= window_end <= end:
                continue
            end_text = window_end.isoformat()
            window_days = _int(window.get("window_days"))
            regime_tags = tags_by_end.get((end_text, window_days), [])
            pressure = bool(set(regime_tags) & PRESSURE_VALIDATION_TAGS)
            outcome_status = _text(window.get("outcome_status"))
            rows.append(
                {
                    "outcome_id": _text(source.get("outcome_id")),
                    "daily_advisory_id": _text(
                        window.get("daily_advisory_id") or manifest.get("daily_advisory_id")
                    ),
                    "as_of": _text(window.get("start_date") or manifest.get("as_of")),
                    "window_days": window_days,
                    "outcome_status": outcome_status,
                    "regime_tags": regime_tags,
                    "pressure_regime": pressure,
                    "defensive_validation_relevant": outcome_status == "AVAILABLE"
                    and pressure
                    and window_days in {5, 10, 20},
                    "tag_status": (
                        "PASS"
                        if outcome_status == "AVAILABLE" and regime_tags
                        else "INSUFFICIENT_DATA"
                    ),
                    "production_effect": "none",
                    "broker_action_allowed": False,
                }
            )
    return rows


def run_pressure_regime_tagging(
    *,
    start: date,
    end: date,
    config_path: Path = DEFAULT_PRESSURE_REGIME_TAGGING_CONFIG_PATH,
    output_dir: Path = DEFAULT_PRESSURE_REGIME_TAG_DIR,
    advisory_outcome_dir: Path = DEFAULT_ADVISORY_OUTCOME_DIR,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    enforce_data_quality_gate: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _operations_generated_at(generated_at)
    if start > end:
        raise DynamicV3ConfirmationOperationsError("pressure start must not exceed end")
    if end > generated.date():
        raise DynamicV3ConfirmationOperationsError("pressure end must not exceed cutoff date")
    config_validation = _strict_validation(
        validate_pressure_regime_tagging_config(config_path=config_path),
        source_name="pressure regime config",
    )
    config = _read_yaml_config(config_path)
    config_commitment = _operations_file_commitment(config_path)
    prices_commitment = _operations_file_commitment(prices_path)
    rates_commitment = (
        _operations_file_commitment(rates_path)
        if rates_path.is_file()
        else {"path": str(rates_path), "exists": False}
    )
    price_frame = _load_price_frame(prices_path, start=start, end=end)
    if price_frame.empty:
        raise DynamicV3ConfirmationOperationsError("pressure price source has no usable rows")
    outcome_sources, outcome_inventory = _pressure_outcome_sources(
        advisory_outcome_dir=advisory_outcome_dir,
        start=start,
        end=end,
        generated=generated,
    )
    tag_id = _stable_id("pressure-regime-tag", start.isoformat(), end.isoformat(), generated)
    tag_dir = output_dir / tag_id
    if tag_dir.exists():
        raise DynamicV3ConfirmationOperationsError(f"pressure tag already exists: {tag_id}")
    data_quality_report_bytes = b""
    with TemporaryDirectory(prefix="aits-pressure-regime-dq-") as temporary_dir:
        temporary_report_path = Path(temporary_dir) / "validate_data_quality_report.md"
        data_quality_status, preflight_report_path = _pressure_quality_gate(
            as_of=end,
            generated=generated,
            prices_path=prices_path,
            rates_path=rates_path,
            report_path=temporary_report_path,
            enforce=enforce_data_quality_gate,
        )
        if enforce_data_quality_gate and data_quality_status != "PASS":
            raise DynamicV3ConfirmationOperationsError(
                f"pressure data quality gate failed: {data_quality_status}"
            )
        if preflight_report_path:
            resolved_preflight_path = Path(preflight_report_path)
            if not resolved_preflight_path.is_file():
                raise DynamicV3ConfirmationOperationsError(
                    "pressure data quality report missing after PASS"
                )
            data_quality_report_bytes = resolved_preflight_path.read_bytes()
    data_quality_report_path = (
        str(tag_dir / "validate_data_quality_report.md")
        if data_quality_report_bytes
        else ""
    )
    data_quality_report_commitment = (
        {
            "path": data_quality_report_path,
            "size_bytes": len(data_quality_report_bytes),
            "sha256": hashlib.sha256(data_quality_report_bytes).hexdigest(),
        }
        if data_quality_report_bytes
        else {}
    )
    window_tags = _build_regime_window_tags(price_frame, config)
    outcome_tags = _build_outcome_regime_tags_from_sources(
        sources=outcome_sources,
        window_tags=window_tags,
        start=start,
        end=end,
    )
    summary = _pressure_regime_summary(
        window_tags=window_tags,
        outcome_tags=outcome_tags,
        config=config,
        start=start,
        end=end,
    )
    status = "PASS" if window_tags else "INSUFFICIENT_DATA"
    if data_quality_status not in {"PASS", "SKIPPED_EXPLICIT_TEST_FIXTURE"}:
        status = "PASS_WITH_WARNINGS" if window_tags else "INSUFFICIENT_DATA"
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_regime_manifest",
        "tag_id": tag_dir.name,
        "generated_at": generated.isoformat(),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "status": status,
        "data_quality_status": data_quality_status,
        "data_quality_report_path": data_quality_report_path,
        "config_path": str(config_path),
        "pressure_regime_manifest_path": str(tag_dir / "pressure_regime_manifest.json"),
        "regime_window_tags_path": str(tag_dir / "regime_window_tags.jsonl"),
        "outcome_regime_tags_path": str(tag_dir / "outcome_regime_tags.jsonl"),
        "pressure_regime_summary_path": str(tag_dir / "pressure_regime_summary.json"),
        "pressure_regime_input_snapshot_path": str(tag_dir / "pressure_regime_input_snapshot.json"),
        "pressure_regime_report_path": str(tag_dir / "pressure_regime_report.md"),
        "market_regime": "ai_after_chatgpt",
        **_artifact_safety(),
    }
    snapshot = {
        "schema_version": PRESSURE_REGIME_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_regime_input_snapshot",
        "tag_id": tag_id,
        "generated_at": generated.isoformat(),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "config_path": str(config_path),
        "config": config,
        "config_commitment": config_commitment,
        "config_validation": config_validation,
        "prices_path": str(prices_path),
        "prices_commitment": prices_commitment,
        "rates_path": str(rates_path),
        "rates_commitment": rates_commitment,
        "data_quality_status": data_quality_status,
        "data_quality_report_path": data_quality_report_path,
        "data_quality_report_commitment": data_quality_report_commitment,
        "enforce_data_quality_gate": enforce_data_quality_gate,
        "outcome_root": str(advisory_outcome_dir),
        "outcome_inventory": outcome_inventory,
        "outcome_sources": outcome_sources,
        "production_effect": "none",
    }
    report = render_pressure_regime_report(manifest, summary)
    tag_dir.mkdir(parents=True, exist_ok=False)
    if data_quality_report_bytes:
        write_bytes_atomic(Path(data_quality_report_path), data_quality_report_bytes)
    write_json_atomic(tag_dir / "pressure_regime_manifest.json", manifest)
    _write_jsonl(tag_dir / "regime_window_tags.jsonl", window_tags)
    _write_jsonl(tag_dir / "outcome_regime_tags.jsonl", outcome_tags)
    write_json_atomic(tag_dir / "pressure_regime_summary.json", summary)
    write_json_atomic(tag_dir / "pressure_regime_input_snapshot.json", snapshot)
    write_text_atomic(
        tag_dir / "pressure_regime_report.md",
        report,
    )
    _update_latest_pointer(
        "latest_pressure_regime_tag",
        tag_dir.name,
        tag_dir / "pressure_regime_manifest.json",
    )
    return {
        "tag_id": tag_dir.name,
        "tag_dir": tag_dir,
        "manifest": manifest,
        "regime_window_tags": window_tags,
        "outcome_regime_tags": outcome_tags,
        "pressure_regime_summary": summary,
        "input_snapshot": snapshot,
    }


def pressure_regime_tag_report_payload(
    *,
    tag_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PRESSURE_REGIME_TAG_DIR,
) -> dict[str, Any]:
    tag_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=tag_id if not latest else None,
        pointer_name="latest_pressure_regime_tag",
    )
    return {
        **_read_json(tag_dir / "pressure_regime_manifest.json"),
        "regime_window_tags": _read_jsonl(tag_dir / "regime_window_tags.jsonl"),
        "outcome_regime_tags": _read_jsonl(tag_dir / "outcome_regime_tags.jsonl"),
        "pressure_regime_summary": _read_json(tag_dir / "pressure_regime_summary.json"),
        **_report_input_snapshot(tag_dir / "pressure_regime_input_snapshot.json"),
        "tag_dir": str(tag_dir),
    }


def validate_pressure_regime_tag_artifact(
    *, tag_id: str, output_dir: Path = DEFAULT_PRESSURE_REGIME_TAG_DIR
) -> dict[str, Any]:
    tag_dir = output_dir / tag_id
    manifest = _read_optional_json(tag_dir / "pressure_regime_manifest.json") or {}
    window_tags = _read_jsonl(tag_dir / "regime_window_tags.jsonl")
    outcome_tags = _read_jsonl(tag_dir / "outcome_regime_tags.jsonl")
    summary = _read_optional_json(tag_dir / "pressure_regime_summary.json") or {}
    snapshot = _read_optional_json(tag_dir / "pressure_regime_input_snapshot.json") or {}
    valid_tags = set(PRESSURE_TAGS)
    source_errors: list[str] = []
    recompute_error = ""
    expected_window_tags: list[dict[str, Any]] = []
    expected_outcome_tags: list[dict[str, Any]] = []
    expected_summary: dict[str, Any] = {}
    expected_manifest: dict[str, Any] = {}
    expected_report = ""
    try:
        if snapshot.get("schema_version") != PRESSURE_REGIME_SNAPSHOT_SCHEMA_VERSION:
            raise DynamicV3ConfirmationOperationsError("pressure snapshot schema mismatch")
        generated = _operations_datetime(
            snapshot.get("generated_at"), field="pressure generated_at"
        )
        start = date.fromisoformat(_text(snapshot.get("start")))
        end = date.fromisoformat(_text(snapshot.get("end")))
        config_path = Path(_text(snapshot.get("config_path")))
        prices_path = Path(_text(snapshot.get("prices_path")))
        rates_path = Path(_text(snapshot.get("rates_path")))
        if _operations_file_commitment(config_path) != _mapping(snapshot.get("config_commitment")):
            source_errors.append("pressure config commitment drift")
        if _operations_file_commitment(prices_path) != _mapping(snapshot.get("prices_commitment")):
            source_errors.append("pressure prices commitment drift")
        rates_commitment = _mapping(snapshot.get("rates_commitment"))
        if rates_commitment.get("exists") is False:
            if rates_path.exists():
                source_errors.append("pressure rates source appearance drift")
        elif _operations_file_commitment(rates_path) != rates_commitment:
            source_errors.append("pressure rates commitment drift")
        live_config_validation = _strict_validation(
            validate_pressure_regime_tagging_config(config_path=config_path),
            source_name="pressure regime config",
        )
        if live_config_validation != _mapping(snapshot.get("config_validation")):
            source_errors.append("pressure config validation drift")
        dq_commitment = _mapping(snapshot.get("data_quality_report_commitment"))
        dq_path = _text(snapshot.get("data_quality_report_path"))
        if dq_commitment and _operations_file_commitment(Path(dq_path)) != dq_commitment:
            source_errors.append("pressure data quality report drift")
        live_sources, live_inventory = _pressure_outcome_sources(
            advisory_outcome_dir=Path(_text(snapshot.get("outcome_root"))),
            start=start,
            end=end,
            generated=generated,
        )
        if live_inventory != _records(snapshot.get("outcome_inventory")):
            source_errors.append("pressure outcome inventory drift")
        frozen_sources = _records(snapshot.get("outcome_sources"))
        if [row.get("outcome_id") for row in live_sources] != [
            row.get("outcome_id") for row in frozen_sources
        ]:
            source_errors.append("pressure selected outcome set drift")
        for source in frozen_sources:
            source_errors.extend(_validate_operations_source_bundle(_mapping(source.get("bundle"))))
            live_validation = _strict_validation(
                validate_advisory_outcome_artifact(
                    outcome_id=_text(source.get("outcome_id")),
                    output_dir=Path(_text(snapshot.get("outcome_root"))),
                ),
                source_name=f"advisory outcome {source.get('outcome_id')}",
            )
            if live_validation != _mapping(source.get("validation")):
                source_errors.append(
                    f"pressure outcome validation drift: {source.get('outcome_id')}"
                )
        config = _mapping(snapshot.get("config"))
        price_frame = _load_price_frame(prices_path, start=start, end=end)
        expected_window_tags = _build_regime_window_tags(price_frame, config)
        expected_outcome_tags = _build_outcome_regime_tags_from_sources(
            sources=frozen_sources,
            window_tags=expected_window_tags,
            start=start,
            end=end,
        )
        expected_summary = _pressure_regime_summary(
            window_tags=expected_window_tags,
            outcome_tags=expected_outcome_tags,
            config=config,
            start=start,
            end=end,
        )
        status = "PASS" if expected_window_tags else "INSUFFICIENT_DATA"
        expected_manifest = {
            "schema_version": SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_pressure_regime_manifest",
            "tag_id": tag_id,
            "generated_at": generated.isoformat(),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "status": status,
            "data_quality_status": snapshot.get("data_quality_status"),
            "data_quality_report_path": snapshot.get("data_quality_report_path"),
            "config_path": str(config_path),
            "pressure_regime_manifest_path": str(tag_dir / "pressure_regime_manifest.json"),
            "regime_window_tags_path": str(tag_dir / "regime_window_tags.jsonl"),
            "outcome_regime_tags_path": str(tag_dir / "outcome_regime_tags.jsonl"),
            "pressure_regime_summary_path": str(tag_dir / "pressure_regime_summary.json"),
            "pressure_regime_input_snapshot_path": str(
                tag_dir / "pressure_regime_input_snapshot.json"
            ),
            "pressure_regime_report_path": str(tag_dir / "pressure_regime_report.md"),
            "market_regime": "ai_after_chatgpt",
            **_artifact_safety(),
        }
        expected_report = render_pressure_regime_report(expected_manifest, expected_summary)
    except Exception as exc:  # noqa: BLE001
        recompute_error = str(exc)
    checks = [
        _check("manifest_exists", (tag_dir / "pressure_regime_manifest.json").exists(), ""),
        _check("window_tags_exists", (tag_dir / "regime_window_tags.jsonl").exists(), ""),
        _check("outcome_tags_exists", (tag_dir / "outcome_regime_tags.jsonl").exists(), ""),
        _check("summary_exists", (tag_dir / "pressure_regime_summary.json").exists(), ""),
        _check("report_exists", (tag_dir / "pressure_regime_report.md").exists(), ""),
        _check(
            "input_snapshot_exists",
            (tag_dir / "pressure_regime_input_snapshot.json").exists(),
            "",
        ),
        _check("tag_id_matches", manifest.get("tag_id") == tag_id, ""),
        _check(
            "window_tags_valid",
            all(
                set(_records_to_texts(row.get("regime_tags"))) <= valid_tags for row in window_tags
            ),
            "known pressure tags",
        ),
        _check(
            "outcome_tag_status_valid",
            all(
                row.get("tag_status") in {"PASS", "PASS_WITH_WARNINGS", "INSUFFICIENT_DATA"}
                for row in outcome_tags
            ),
            "outcome tag status",
        ),
        _check(
            "summary_counts_present",
            all(tag in _mapping(summary.get("pressure_samples")) for tag in PRESSURE_TAGS),
            "pressure sample buckets",
        ),
        _check("source_recompute_succeeded", not recompute_error, recompute_error),
        _check("source_bundles_valid", not source_errors, "; ".join(source_errors)),
        _check("window_tags_recomputed", window_tags == expected_window_tags, ""),
        _check("outcome_tags_recomputed", outcome_tags == expected_outcome_tags, ""),
        _check("summary_recomputed", summary == expected_summary, ""),
        _check("manifest_recomputed", manifest == expected_manifest, ""),
        _check(
            "report_recomputed",
            _read_text(tag_dir / "pressure_regime_report.md") == expected_report,
            "",
        ),
        _check(
            "safety_no_broker",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker disabled",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_pressure_regime_tag_validation",
        artifact_id_key="tag_id",
        artifact_id=tag_id,
        checks=checks,
    )


def _dashboard_source_validation(*, kind: str, artifact_id: str, root: Path) -> dict[str, Any]:
    validators = {
        "weekly": lambda: validate_confirmation_cycle_weekly_artifact(
            weekly_cycle_id=artifact_id, output_dir=root
        ),
        "progress": lambda: validate_confirmation_progress_artifact(
            progress_id=artifact_id, output_dir=root
        ),
        "evaluation": lambda: validate_confirmation_evaluation_artifact(
            evaluation_id=artifact_id, output_dir=root
        ),
        "rule_cycle": lambda: validate_rule_review_cycle_artifact(
            cycle_id=artifact_id, output_dir=root
        ),
        "queue": lambda: validate_rule_review_queue_artifact(queue_id=artifact_id, output_dir=root),
        "pressure": lambda: validate_pressure_regime_tag_artifact(
            tag_id=artifact_id, output_dir=root
        ),
    }
    validator = validators.get(kind)
    if validator is None:
        raise DynamicV3ConfirmationOperationsError(f"unknown dashboard source: {kind}")
    return _strict_validation(validator(), source_name=f"dashboard {kind}")


def _dashboard_source_record(
    *,
    kind: str,
    artifact_id: str | None,
    root: Path,
    manifest_name: str,
    id_key: str,
    generated: datetime,
    required: bool,
) -> dict[str, Any]:
    resolved_id = _semantic_artifact_id(
        output_dir=root,
        artifact_id=artifact_id,
        manifest_name=manifest_name,
        id_key=id_key,
        generated=generated,
        source_name=f"dashboard {kind}",
        required=required,
    )
    if not resolved_id:
        return {
            "kind": kind,
            "selection_status": "ABSENT",
            "artifact_id": "",
            "root": str(root),
        }
    validation = _dashboard_source_validation(kind=kind, artifact_id=resolved_id, root=root)
    artifact_dir = root / resolved_id
    names = sorted(path.name for path in artifact_dir.iterdir() if path.is_file())
    bundle = _operations_source_bundle(
        source_dir=artifact_dir,
        canonical_files=names,
        json_views=[name for name in names if name.endswith(".json") and "snapshot" not in name],
        jsonl_views=[name for name in names if name.endswith(".jsonl")],
    )
    manifest = _mapping(_mapping(bundle.get("json")).get(manifest_name))
    source_generated = _source_not_after_cutoff(
        manifest, generated=generated, source_name=f"dashboard {kind}"
    )
    return {
        "kind": kind,
        "selection_status": "SELECTED",
        "artifact_id": resolved_id,
        "root": str(root),
        "generated_at": source_generated.isoformat(),
        "validation": validation,
        "bundle": bundle,
    }


def _dashboard_payload(source: Mapping[str, Any]) -> dict[str, Any]:
    if source.get("selection_status") != "SELECTED":
        return {}
    kind = _text(source.get("kind"))
    bundle = _mapping(source.get("bundle"))
    json_views = _mapping(bundle.get("json"))
    jsonl_views = _mapping(bundle.get("jsonl"))
    manifests = [_mapping(value) for name, value in json_views.items() if "manifest" in name]
    if len(manifests) != 1:
        raise DynamicV3ConfirmationOperationsError(
            f"dashboard {kind} source manifest count invalid"
        )
    payload = dict(manifests[0])
    mappings: dict[str, tuple[dict[str, Any], str]] = {
        "progress": (
            {
                "target_progress": _records(jsonl_views.get("target_progress.jsonl")),
                "target_progress_summary": _mapping(json_views.get("target_progress_summary.json")),
            },
            "progress_id",
        ),
        "evaluation": (
            {
                "target_evaluations": _records(jsonl_views.get("target_evaluations.jsonl")),
                "confirmation_evaluation_summary": _mapping(
                    json_views.get("confirmation_evaluation_summary.json")
                ),
            },
            "evaluation_id",
        ),
        "rule_cycle": (
            {
                "rule_review_decision_matrix": _mapping(
                    json_views.get("rule_review_decision_matrix.json")
                )
            },
            "cycle_id",
        ),
        "queue": (
            {
                "queue_items": _records(jsonl_views.get("queue_items.jsonl")),
                "queue_summary": _mapping(json_views.get("queue_summary.json")),
            },
            "queue_id",
        ),
        "pressure": (
            {
                "regime_window_tags": _records(jsonl_views.get("regime_window_tags.jsonl")),
                "outcome_regime_tags": _records(jsonl_views.get("outcome_regime_tags.jsonl")),
                "pressure_regime_summary": _mapping(json_views.get("pressure_regime_summary.json")),
            },
            "tag_id",
        ),
        "weekly": (
            {
                "weekly_cycle_steps": _mapping(json_views.get("weekly_cycle_steps.json")),
                "weekly_cycle_artifacts": _mapping(json_views.get("weekly_cycle_artifacts.json")),
                "weekly_cycle_summary": _mapping(json_views.get("weekly_cycle_summary.json")),
            },
            "weekly_cycle_id",
        ),
    }
    extra, _ = mappings[kind]
    payload.update(extra)
    return payload


def _dashboard_validate_lineage(sources: Sequence[Mapping[str, Any]]) -> None:
    by_kind = {_text(source.get("kind")): source for source in sources}
    progress = _dashboard_payload(by_kind["progress"])
    evaluation = _dashboard_payload(by_kind["evaluation"])
    cycle = _dashboard_payload(by_kind["rule_cycle"])
    queue = _dashboard_payload(by_kind["queue"])
    progress_id = _text(progress.get("progress_id"))
    evaluation_id = _text(evaluation.get("evaluation_id"))
    cycle_id = _text(cycle.get("cycle_id"))
    if (
        evaluation.get("progress_id") != progress_id
        or cycle.get("progress_id") != progress_id
        or cycle.get("evaluation_id") != evaluation_id
        or queue.get("source_cycle_id") != cycle_id
    ):
        raise DynamicV3ConfirmationOperationsError("dashboard source lineage mismatch")
    target_sets = [
        {_text(row.get("target_id")) for row in _records(progress.get("target_progress"))},
        {_text(row.get("target_id")) for row in _records(evaluation.get("target_evaluations"))},
        {
            _text(row.get("target_id"))
            for row in _records(_mapping(cycle.get("rule_review_decision_matrix")).get("targets"))
        },
        {_text(row.get("target_id")) for row in _records(queue.get("queue_items"))},
    ]
    if not target_sets[0] or any(targets != target_sets[0] for targets in target_sets[1:]):
        raise DynamicV3ConfirmationOperationsError("dashboard target coverage mismatch")
    weekly_source = by_kind.get("weekly")
    if weekly_source and weekly_source.get("selection_status") == "SELECTED":
        weekly = _dashboard_payload(weekly_source)
        artifacts = _mapping(weekly.get("weekly_cycle_artifacts"))
        if (
            artifacts.get("confirmation_progress_id") != progress_id
            or artifacts.get("confirmation_evaluation_id") != evaluation_id
            or artifacts.get("rule_review_cycle_id") != cycle_id
            or artifacts.get("rule_review_queue_id") != queue.get("queue_id")
        ):
            raise DynamicV3ConfirmationOperationsError("dashboard weekly source lineage mismatch")


@with_artifact_validation_session
def build_confirmation_dashboard(
    *,
    week_ending: date,
    weekly_cycle_id: str | None = None,
    weekly_cycle_reference_id: str | None = None,
    progress_id: str | None = None,
    evaluation_id: str | None = None,
    cycle_id: str | None = None,
    queue_id: str | None = None,
    output_dir: Path = DEFAULT_CONFIRMATION_DASHBOARD_DIR,
    weekly_cycle_dir: Path = DEFAULT_CONFIRMATION_CYCLE_WEEKLY_DIR,
    progress_dir: Path = DEFAULT_CONFIRMATION_PROGRESS_DIR,
    evaluation_dir: Path = DEFAULT_CONFIRMATION_EVALUATION_DIR,
    rule_cycle_dir: Path = DEFAULT_RULE_REVIEW_CYCLE_DIR,
    queue_dir: Path = DEFAULT_RULE_REVIEW_QUEUE_DIR,
    pressure_tag_dir: Path = DEFAULT_PRESSURE_REGIME_TAG_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _operations_generated_at(generated_at)
    sources = [
        _dashboard_source_record(
            kind="weekly",
            artifact_id=weekly_cycle_id,
            root=weekly_cycle_dir,
            manifest_name="weekly_cycle_manifest.json",
            id_key="weekly_cycle_id",
            generated=generated,
            required=False,
        ),
        _dashboard_source_record(
            kind="progress",
            artifact_id=progress_id,
            root=progress_dir,
            manifest_name="confirmation_progress_manifest.json",
            id_key="progress_id",
            generated=generated,
            required=True,
        ),
        _dashboard_source_record(
            kind="evaluation",
            artifact_id=evaluation_id,
            root=evaluation_dir,
            manifest_name="confirmation_evaluation_manifest.json",
            id_key="evaluation_id",
            generated=generated,
            required=True,
        ),
        _dashboard_source_record(
            kind="rule_cycle",
            artifact_id=cycle_id,
            root=rule_cycle_dir,
            manifest_name="rule_review_cycle_manifest.json",
            id_key="cycle_id",
            generated=generated,
            required=True,
        ),
        _dashboard_source_record(
            kind="queue",
            artifact_id=queue_id,
            root=queue_dir,
            manifest_name="rule_review_queue_manifest.json",
            id_key="queue_id",
            generated=generated,
            required=True,
        ),
        _dashboard_source_record(
            kind="pressure",
            artifact_id=None,
            root=pressure_tag_dir,
            manifest_name="pressure_regime_manifest.json",
            id_key="tag_id",
            generated=generated,
            required=False,
        ),
    ]
    _dashboard_validate_lineage(sources)
    by_kind = {_text(source.get("kind")): source for source in sources}
    weekly = _dashboard_payload(by_kind["weekly"])
    progress = _dashboard_payload(by_kind["progress"])
    evaluation = _dashboard_payload(by_kind["evaluation"])
    rule_cycle = _dashboard_payload(by_kind["rule_cycle"])
    queue = _dashboard_payload(by_kind["queue"])
    pressure = _dashboard_payload(by_kind["pressure"])
    target_table = _dashboard_target_status_table(progress, evaluation, pressure)
    pressure_dashboard = _pressure_sample_dashboard(pressure)
    summary = _confirmation_dashboard_summary(
        week_ending=week_ending,
        target_table=target_table,
        rule_cycle=rule_cycle,
        queue=queue,
    )
    dashboard_id = _stable_id(
        "confirmation-dashboard",
        week_ending.isoformat(),
        generated.isoformat(),
    )
    dashboard_dir = output_dir / dashboard_id
    if dashboard_dir.exists():
        raise DynamicV3ConfirmationOperationsError(
            f"confirmation dashboard already exists: {dashboard_id}"
        )
    reference_weekly_id = _text(
        weekly_cycle_reference_id or weekly_cycle_id or weekly.get("weekly_cycle_id")
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_dashboard_manifest",
        "dashboard_id": dashboard_dir.name,
        "weekly_cycle_id": reference_weekly_id,
        "week_ending": week_ending.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS" if target_table["targets"] else "INSUFFICIENT_DATA",
        "confirmation_dashboard_manifest_path": str(
            dashboard_dir / "confirmation_dashboard_manifest.json"
        ),
        "target_status_table_path": str(dashboard_dir / "target_status_table.json"),
        "pressure_sample_dashboard_path": str(dashboard_dir / "pressure_sample_dashboard.json"),
        "confirmation_dashboard_summary_path": str(
            dashboard_dir / "confirmation_dashboard_summary.json"
        ),
        "confirmation_dashboard_input_snapshot_path": str(
            dashboard_dir / "confirmation_dashboard_input_snapshot.json"
        ),
        "confirmation_dashboard_report_path": str(
            dashboard_dir / "confirmation_dashboard_report.md"
        ),
        "reader_brief_section_path": str(dashboard_dir / "reader_brief_section.md"),
        **_artifact_safety(),
    }
    snapshot = {
        "schema_version": CONFIRMATION_DASHBOARD_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_dashboard_input_snapshot",
        "dashboard_id": dashboard_id,
        "week_ending": week_ending.isoformat(),
        "generated_at": generated.isoformat(),
        "weekly_cycle_reference_id": reference_weekly_id,
        "sources": sources,
        "production_effect": "none",
    }
    report = render_confirmation_dashboard_report(
        manifest, target_table, pressure_dashboard, summary
    )
    reader_brief = render_confirmation_dashboard_reader_brief(
        summary, target_table, pressure_dashboard
    )
    dashboard_dir.mkdir(parents=True, exist_ok=False)
    write_json_atomic(dashboard_dir / "confirmation_dashboard_manifest.json", manifest)
    write_json_atomic(dashboard_dir / "target_status_table.json", target_table)
    write_json_atomic(dashboard_dir / "pressure_sample_dashboard.json", pressure_dashboard)
    write_json_atomic(dashboard_dir / "confirmation_dashboard_summary.json", summary)
    write_json_atomic(dashboard_dir / "confirmation_dashboard_input_snapshot.json", snapshot)
    write_text_atomic(
        dashboard_dir / "confirmation_dashboard_report.md",
        report,
    )
    write_text_atomic(
        dashboard_dir / "reader_brief_section.md",
        reader_brief,
    )
    _update_latest_pointer(
        "latest_confirmation_dashboard",
        dashboard_dir.name,
        dashboard_dir / "confirmation_dashboard_manifest.json",
    )
    return {
        "dashboard_id": dashboard_dir.name,
        "dashboard_dir": dashboard_dir,
        "manifest": manifest,
        "target_status_table": target_table,
        "pressure_sample_dashboard": pressure_dashboard,
        "confirmation_dashboard_summary": summary,
        "input_snapshot": snapshot,
    }


def confirmation_dashboard_report_payload(
    *,
    dashboard_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CONFIRMATION_DASHBOARD_DIR,
) -> dict[str, Any]:
    dashboard_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=dashboard_id if not latest else None,
        pointer_name="latest_confirmation_dashboard",
    )
    return {
        **_read_json(dashboard_dir / "confirmation_dashboard_manifest.json"),
        "target_status_table": _read_json(dashboard_dir / "target_status_table.json"),
        "pressure_sample_dashboard": _read_json(dashboard_dir / "pressure_sample_dashboard.json"),
        "confirmation_dashboard_summary": _read_json(
            dashboard_dir / "confirmation_dashboard_summary.json"
        ),
        **_report_input_snapshot(dashboard_dir / "confirmation_dashboard_input_snapshot.json"),
        "reader_brief_section": _read_text(dashboard_dir / "reader_brief_section.md"),
        "dashboard_dir": str(dashboard_dir),
    }


@with_artifact_validation_session
def validate_confirmation_dashboard_artifact(
    *, dashboard_id: str, output_dir: Path = DEFAULT_CONFIRMATION_DASHBOARD_DIR
) -> dict[str, Any]:
    dashboard_dir = output_dir / dashboard_id
    manifest = _read_optional_json(dashboard_dir / "confirmation_dashboard_manifest.json") or {}
    target_table = _read_optional_json(dashboard_dir / "target_status_table.json") or {}
    pressure = _read_optional_json(dashboard_dir / "pressure_sample_dashboard.json") or {}
    summary = _read_optional_json(dashboard_dir / "confirmation_dashboard_summary.json") or {}
    snapshot = (
        _read_optional_json(dashboard_dir / "confirmation_dashboard_input_snapshot.json") or {}
    )
    targets = _records(target_table.get("targets"))
    source_errors: list[str] = []
    recompute_error = ""
    expected_target_table: dict[str, Any] = {}
    expected_pressure: dict[str, Any] = {}
    expected_summary: dict[str, Any] = {}
    expected_manifest: dict[str, Any] = {}
    expected_report = ""
    expected_reader_brief = ""
    try:
        if snapshot.get("schema_version") != CONFIRMATION_DASHBOARD_SNAPSHOT_SCHEMA_VERSION:
            raise DynamicV3ConfirmationOperationsError("dashboard snapshot schema mismatch")
        generated = _operations_datetime(
            snapshot.get("generated_at"), field="dashboard generated_at"
        )
        week_ending = date.fromisoformat(_text(snapshot.get("week_ending")))
        sources = _records(snapshot.get("sources"))
        by_kind = {_text(source.get("kind")): source for source in sources}
        if set(by_kind) != {
            "weekly",
            "progress",
            "evaluation",
            "rule_cycle",
            "queue",
            "pressure",
        } or len(by_kind) != len(sources):
            raise DynamicV3ConfirmationOperationsError(
                "dashboard source kinds missing or duplicate"
            )
        for source in sources:
            if source.get("selection_status") == "ABSENT":
                if source.get("kind") not in {"weekly", "pressure"}:
                    source_errors.append(f"required dashboard source absent: {source.get('kind')}")
                continue
            kind = _text(source.get("kind"))
            artifact_id = _text(source.get("artifact_id"))
            root = Path(_text(source.get("root")))
            live_validation = _dashboard_source_validation(
                kind=kind, artifact_id=artifact_id, root=root
            )
            if live_validation != _mapping(source.get("validation")):
                source_errors.append(f"dashboard source validation drift: {kind}")
            source_errors.extend(_validate_operations_source_bundle(_mapping(source.get("bundle"))))
            source_generated = _operations_datetime(
                source.get("generated_at"), field=f"dashboard {kind} generated_at"
            )
            if source_generated > generated:
                source_errors.append(f"dashboard source after cutoff: {kind}")
        _dashboard_validate_lineage(sources)
        progress_payload = _dashboard_payload(by_kind["progress"])
        evaluation_payload = _dashboard_payload(by_kind["evaluation"])
        cycle_payload = _dashboard_payload(by_kind["rule_cycle"])
        queue_payload = _dashboard_payload(by_kind["queue"])
        pressure_payload = _dashboard_payload(by_kind["pressure"])
        expected_target_table = _dashboard_target_status_table(
            progress_payload, evaluation_payload, pressure_payload
        )
        expected_pressure = _pressure_sample_dashboard(pressure_payload)
        expected_summary = _confirmation_dashboard_summary(
            week_ending=week_ending,
            target_table=expected_target_table,
            rule_cycle=cycle_payload,
            queue=queue_payload,
        )
        expected_manifest = {
            "schema_version": SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_confirmation_dashboard_manifest",
            "dashboard_id": dashboard_id,
            "weekly_cycle_id": snapshot.get("weekly_cycle_reference_id"),
            "week_ending": week_ending.isoformat(),
            "generated_at": generated.isoformat(),
            "status": "PASS" if expected_target_table["targets"] else "INSUFFICIENT_DATA",
            "confirmation_dashboard_manifest_path": str(
                dashboard_dir / "confirmation_dashboard_manifest.json"
            ),
            "target_status_table_path": str(dashboard_dir / "target_status_table.json"),
            "pressure_sample_dashboard_path": str(dashboard_dir / "pressure_sample_dashboard.json"),
            "confirmation_dashboard_summary_path": str(
                dashboard_dir / "confirmation_dashboard_summary.json"
            ),
            "confirmation_dashboard_input_snapshot_path": str(
                dashboard_dir / "confirmation_dashboard_input_snapshot.json"
            ),
            "confirmation_dashboard_report_path": str(
                dashboard_dir / "confirmation_dashboard_report.md"
            ),
            "reader_brief_section_path": str(dashboard_dir / "reader_brief_section.md"),
            **_artifact_safety(),
        }
        expected_report = render_confirmation_dashboard_report(
            expected_manifest,
            expected_target_table,
            expected_pressure,
            expected_summary,
        )
        expected_reader_brief = render_confirmation_dashboard_reader_brief(
            expected_summary, expected_target_table, expected_pressure
        )
    except Exception as exc:  # noqa: BLE001
        recompute_error = str(exc)
    checks = [
        _check(
            "manifest_exists",
            (dashboard_dir / "confirmation_dashboard_manifest.json").exists(),
            "",
        ),
        _check("target_table_exists", (dashboard_dir / "target_status_table.json").exists(), ""),
        _check(
            "pressure_dashboard_exists",
            (dashboard_dir / "pressure_sample_dashboard.json").exists(),
            "",
        ),
        _check(
            "summary_exists",
            (dashboard_dir / "confirmation_dashboard_summary.json").exists(),
            "",
        ),
        _check("report_exists", (dashboard_dir / "confirmation_dashboard_report.md").exists(), ""),
        _check("reader_brief_exists", (dashboard_dir / "reader_brief_section.md").exists(), ""),
        _check(
            "input_snapshot_exists",
            (dashboard_dir / "confirmation_dashboard_input_snapshot.json").exists(),
            "",
        ),
        _check("dashboard_id_matches", manifest.get("dashboard_id") == dashboard_id, ""),
        _check("targets_present", bool(targets), "target table"),
        _check(
            "pressure_buckets_present",
            all(tag in _mapping(pressure.get("pressure_samples")) for tag in PRESSURE_TAGS),
            "pressure buckets",
        ),
        _check(
            "policy_change_disallowed",
            summary.get("policy_change_allowed") is False,
            "policy_change_allowed=false",
        ),
        _check("source_recompute_succeeded", not recompute_error, recompute_error),
        _check("source_bundles_valid", not source_errors, "; ".join(source_errors)),
        _check("target_table_recomputed", target_table == expected_target_table, ""),
        _check("pressure_dashboard_recomputed", pressure == expected_pressure, ""),
        _check("summary_recomputed", summary == expected_summary, ""),
        _check("manifest_recomputed", manifest == expected_manifest, ""),
        _check(
            "report_recomputed",
            _read_text(dashboard_dir / "confirmation_dashboard_report.md") == expected_report,
            "",
        ),
        _check(
            "reader_brief_recomputed",
            _read_text(dashboard_dir / "reader_brief_section.md") == expected_reader_brief,
            "",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker disabled",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_confirmation_dashboard_validation",
        artifact_id_key="dashboard_id",
        artifact_id=dashboard_id,
        checks=checks,
    )


def _queue_owner_snapshot(
    *, journal_path: Path, cycle_id: str, generated: datetime
) -> dict[str, Any]:
    listing = list_rule_owner_decisions(journal_path=journal_path)
    legacy = listing.get("legacy_unsnapshotted") is True
    records: list[dict[str, Any]] = []
    validations: dict[str, Any] = {}
    if listing.get("status") == "PASS":
        for row in _records(listing.get("records")):
            decision_id = _text(row.get("decision_id"))
            validation = _strict_validation(
                validate_rule_owner_decision_artifact(
                    decision_id=decision_id, journal_path=journal_path
                ),
                source_name=f"owner decision {decision_id}",
            )
            validations[decision_id] = validation
            if _text(row.get("cycle_id")) != cycle_id:
                continue
            event_time = _operations_datetime(
                row.get("recorded_at") or row.get("created_at"),
                field=f"owner decision {decision_id} event time",
            )
            if event_time > generated:
                raise DynamicV3ConfirmationOperationsError(
                    f"owner decision generated after queue cutoff: {decision_id}"
                )
            records.append(dict(row))
    elif listing.get("status") not in {"MISSING", "PASS_WITH_WARNINGS"}:
        raise DynamicV3ConfirmationOperationsError("owner decision journal validation failed")
    commitment: dict[str, Any] = (
        _operations_file_commitment(journal_path)
        if journal_path.is_file()
        else {"path": str(journal_path), "exists": False}
    )
    return {
        "journal_path": str(journal_path),
        "journal_commitment": commitment,
        "listing_status": listing.get("status"),
        "legacy_unsnapshotted_ignored": legacy,
        "selected_cycle_id": cycle_id,
        "selected_records": records,
        "selected_validations": validations,
        "production_effect": "none",
    }


def _queue_cycle_payload(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    bundle = _mapping(snapshot.get("cycle_bundle"))
    json_views = _mapping(bundle.get("json"))
    manifest = _mapping(json_views.get("rule_review_cycle_manifest.json"))
    matrix = _mapping(json_views.get("rule_review_decision_matrix.json"))
    return {**manifest, "rule_review_decision_matrix": matrix}


def _queue_items_from_snapshot(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    cycle_payload = _queue_cycle_payload(snapshot)
    matrix = _mapping(cycle_payload.get("rule_review_decision_matrix"))
    decisions = _records(_mapping(snapshot.get("owner_decisions")).get("selected_records"))
    return [_queue_item(row, decisions, cycle_payload) for row in _records(matrix.get("targets"))]


@with_artifact_validation_session
def build_rule_review_queue(
    *,
    cycle_id: str | None = None,
    output_dir: Path = DEFAULT_RULE_REVIEW_QUEUE_DIR,
    cycle_dir: Path = DEFAULT_RULE_REVIEW_CYCLE_DIR,
    journal_path: Path = DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _operations_generated_at(generated_at)
    resolved_cycle_id = _semantic_artifact_id(
        output_dir=cycle_dir,
        artifact_id=cycle_id,
        manifest_name="rule_review_cycle_manifest.json",
        id_key="cycle_id",
        generated=generated,
        source_name="rule review queue cycle",
        required=True,
    )
    cycle_validation = _strict_validation(
        validate_rule_review_cycle_artifact(cycle_id=resolved_cycle_id, output_dir=cycle_dir),
        source_name="rule review queue cycle",
    )
    cycle_artifact_dir = cycle_dir / resolved_cycle_id
    cycle_bundle = _operations_source_bundle(
        source_dir=cycle_artifact_dir,
        json_views=(
            "rule_review_cycle_manifest.json",
            "rule_review_decision_matrix.json",
        ),
    )
    cycle_manifest = _mapping(
        _mapping(cycle_bundle.get("json")).get("rule_review_cycle_manifest.json")
    )
    _source_not_after_cutoff(
        cycle_manifest, generated=generated, source_name="rule review queue cycle"
    )
    owner_snapshot = _queue_owner_snapshot(
        journal_path=journal_path,
        cycle_id=resolved_cycle_id,
        generated=generated,
    )
    queue_id = _stable_id(
        "rule-review-queue",
        resolved_cycle_id,
        generated.isoformat(),
    )
    queue_dir = output_dir / queue_id
    if queue_dir.exists():
        raise DynamicV3ConfirmationOperationsError(f"rule review queue exists: {queue_id}")
    snapshot = {
        "schema_version": RULE_REVIEW_QUEUE_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_rule_review_queue_input_snapshot",
        "queue_id": queue_id,
        "generated_at": generated.isoformat(),
        "cycle_id": resolved_cycle_id,
        "cycle_root": str(cycle_dir),
        "cycle_validation": cycle_validation,
        "cycle_bundle": cycle_bundle,
        "owner_decisions": owner_snapshot,
        "production_effect": "none",
    }
    items = _queue_items_from_snapshot(snapshot)
    summary = _queue_summary(items, generated)
    summary["queue_id"] = queue_dir.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_rule_review_queue_manifest",
        "queue_id": queue_dir.name,
        "source_cycle_id": resolved_cycle_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if items else "INSUFFICIENT_DATA",
        "rule_review_queue_manifest_path": str(queue_dir / "rule_review_queue_manifest.json"),
        "queue_items_path": str(queue_dir / "queue_items.jsonl"),
        "queue_summary_path": str(queue_dir / "queue_summary.json"),
        "rule_review_queue_input_snapshot_path": str(
            queue_dir / "rule_review_queue_input_snapshot.json"
        ),
        "rule_review_queue_report_path": str(queue_dir / "rule_review_queue_report.md"),
        "policy_change_allowed": False,
        **_artifact_safety(),
    }
    report = render_rule_review_queue_report(manifest, summary, items)
    queue_dir.mkdir(parents=True, exist_ok=False)
    write_json_atomic(queue_dir / "rule_review_queue_manifest.json", manifest)
    _write_jsonl(queue_dir / "queue_items.jsonl", items)
    write_json_atomic(queue_dir / "queue_summary.json", summary)
    write_json_atomic(queue_dir / "rule_review_queue_input_snapshot.json", snapshot)
    write_text_atomic(
        queue_dir / "rule_review_queue_report.md",
        report,
    )
    _update_latest_pointer(
        "latest_rule_review_queue",
        queue_dir.name,
        queue_dir / "rule_review_queue_manifest.json",
    )
    return {
        "queue_id": queue_dir.name,
        "queue_dir": queue_dir,
        "manifest": manifest,
        "queue_items": items,
        "queue_summary": summary,
        "input_snapshot": snapshot,
    }


def rule_review_queue_report_payload(
    *,
    queue_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_RULE_REVIEW_QUEUE_DIR,
) -> dict[str, Any]:
    queue_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=queue_id if not latest else None,
        pointer_name="latest_rule_review_queue",
    )
    return {
        **_read_json(queue_dir / "rule_review_queue_manifest.json"),
        "queue_items": _read_jsonl(queue_dir / "queue_items.jsonl"),
        "queue_summary": _read_json(queue_dir / "queue_summary.json"),
        **_report_input_snapshot(queue_dir / "rule_review_queue_input_snapshot.json"),
        "rule_review_queue_report": _read_text(queue_dir / "rule_review_queue_report.md"),
        "queue_dir": str(queue_dir),
    }


@with_artifact_validation_session
def validate_rule_review_queue_artifact(
    *, queue_id: str, output_dir: Path = DEFAULT_RULE_REVIEW_QUEUE_DIR
) -> dict[str, Any]:
    queue_dir = output_dir / queue_id
    manifest = _read_optional_json(queue_dir / "rule_review_queue_manifest.json") or {}
    items = _read_jsonl(queue_dir / "queue_items.jsonl")
    summary = _read_optional_json(queue_dir / "queue_summary.json") or {}
    snapshot = _read_optional_json(queue_dir / "rule_review_queue_input_snapshot.json") or {}
    allowed_status = {"pending", "reviewed", "deferred", "not_ready"}
    source_errors: list[str] = []
    recompute_error = ""
    expected_items: list[dict[str, Any]] = []
    expected_summary: dict[str, Any] = {}
    expected_manifest: dict[str, Any] = {}
    expected_report = ""
    try:
        if snapshot.get("schema_version") != RULE_REVIEW_QUEUE_SNAPSHOT_SCHEMA_VERSION:
            raise DynamicV3ConfirmationOperationsError("queue snapshot schema mismatch")
        generated = _operations_datetime(snapshot.get("generated_at"), field="queue generated_at")
        cycle_id = _text(snapshot.get("cycle_id"))
        cycle_root = Path(_text(snapshot.get("cycle_root")))
        live_cycle_validation = _strict_validation(
            validate_rule_review_cycle_artifact(cycle_id=cycle_id, output_dir=cycle_root),
            source_name="rule review queue cycle",
        )
        if live_cycle_validation != _mapping(snapshot.get("cycle_validation")):
            source_errors.append("queue cycle validation drift")
        source_errors.extend(
            _validate_operations_source_bundle(_mapping(snapshot.get("cycle_bundle")))
        )
        owner_snapshot = _mapping(snapshot.get("owner_decisions"))
        journal_path = Path(_text(owner_snapshot.get("journal_path")))
        commitment = _mapping(owner_snapshot.get("journal_commitment"))
        if commitment.get("exists") is False:
            if journal_path.exists():
                source_errors.append("queue owner journal appearance drift")
        elif _operations_file_commitment(journal_path) != commitment:
            source_errors.append("queue owner journal commitment drift")
        live_owner_snapshot = _queue_owner_snapshot(
            journal_path=journal_path,
            cycle_id=cycle_id,
            generated=generated,
        )
        if live_owner_snapshot != owner_snapshot:
            source_errors.append("queue owner decision snapshot drift")
        expected_items = _queue_items_from_snapshot(snapshot)
        expected_summary = _queue_summary(expected_items, generated)
        expected_summary["queue_id"] = queue_id
        expected_manifest = {
            "schema_version": SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_rule_review_queue_manifest",
            "queue_id": queue_id,
            "source_cycle_id": cycle_id,
            "generated_at": generated.isoformat(),
            "status": "PASS" if expected_items else "INSUFFICIENT_DATA",
            "rule_review_queue_manifest_path": str(queue_dir / "rule_review_queue_manifest.json"),
            "queue_items_path": str(queue_dir / "queue_items.jsonl"),
            "queue_summary_path": str(queue_dir / "queue_summary.json"),
            "rule_review_queue_input_snapshot_path": str(
                queue_dir / "rule_review_queue_input_snapshot.json"
            ),
            "rule_review_queue_report_path": str(queue_dir / "rule_review_queue_report.md"),
            "policy_change_allowed": False,
            **_artifact_safety(),
        }
        expected_report = render_rule_review_queue_report(
            expected_manifest, expected_summary, expected_items
        )
    except Exception as exc:  # noqa: BLE001
        recompute_error = str(exc)
    checks = [
        _check("manifest_exists", (queue_dir / "rule_review_queue_manifest.json").exists(), ""),
        _check("items_exists", (queue_dir / "queue_items.jsonl").exists(), ""),
        _check("summary_exists", (queue_dir / "queue_summary.json").exists(), ""),
        _check("report_exists", (queue_dir / "rule_review_queue_report.md").exists(), ""),
        _check(
            "input_snapshot_exists",
            (queue_dir / "rule_review_queue_input_snapshot.json").exists(),
            "",
        ),
        _check("queue_id_matches", manifest.get("queue_id") == queue_id, ""),
        _check(
            "queue_status_valid",
            all(row.get("queue_status") in allowed_status for row in items),
            "queue status",
        ),
        _check(
            "not_ready_no_owner_action",
            all(
                row.get("recommended_owner_action") != "manual_policy_review"
                for row in items
                if row.get("queue_status") == "not_ready"
            ),
            "not_ready must not require owner action",
        ),
        _check(
            "policy_change_disallowed",
            manifest.get("policy_change_allowed") is False
            and all(row.get("policy_change_allowed") is False for row in items),
            "policy change disabled",
        ),
        _check(
            "summary_counts_match",
            _int(summary.get("pending_count"))
            + _int(summary.get("reviewed_count"))
            + _int(summary.get("deferred_count"))
            + _int(summary.get("not_ready_count"))
            == len(items),
            "queue counts",
        ),
        _check("source_recompute_succeeded", not recompute_error, recompute_error),
        _check("source_bundles_valid", not source_errors, "; ".join(source_errors)),
        _check("items_recomputed", items == expected_items, ""),
        _check("summary_recomputed", summary == expected_summary, ""),
        _check("manifest_recomputed", manifest == expected_manifest, ""),
        _check(
            "report_recomputed",
            _read_text(queue_dir / "rule_review_queue_report.md") == expected_report,
            "",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker disabled",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_rule_review_queue_validation",
        artifact_id_key="queue_id",
        artifact_id=queue_id,
        checks=checks,
    )


def render_confirmation_cycle_runbook(
    manifest: Mapping[str, Any], command_pack: Mapping[str, Any]
) -> str:
    lines = [
        "# Dynamic Rescue Confirmation Cycle Weekly Runbook",
        "",
        f"- plan_id: `{manifest.get('plan_id')}`",
        "- cadence: `weekly`",
        "- market_regime: `unified_primary_2021`",
        "- default_backtest_start: `2021-02-22`",
        "- broker_action_allowed: `false`",
        "- production_effect: `none`",
        "- auto_apply_policy: `false`",
        "- owner_approval_required: `true`",
        "",
        "## Command Pack",
    ]
    for row in _records(command_pack.get("commands")):
        lines.extend(
            [
                "",
                f"### {_text(row.get('step'))}",
                f"- command: `{row.get('command')}`",
                f"- required: `{row.get('required')}`",
                f"- execution_mode: `{row.get('execution_mode')}`",
                f"- owner_review_required: `{row.get('owner_review_required')}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Safety",
            "",
            "Outcome update is skipped unless `--execute-ready-updates` is supplied.",
            "No command in this pack applies policy, mutates production weights, "
            "or triggers broker action.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_confirmation_cycle_plan_report(
    manifest: Mapping[str, Any], command_pack: Mapping[str, Any]
) -> str:
    review_steps = [
        row["step"]
        for row in _records(command_pack.get("commands"))
        if row.get("owner_review_required") is True
    ]
    return (
        "# Dynamic Rescue Confirmation Cycle Plan\n\n"
        f"- plan_id: `{manifest.get('plan_id')}`\n"
        f"- planned_steps: {manifest.get('planned_step_count')}\n"
        "- dry_run_steps: `outcome_update_if_ready` is skipped unless explicitly enabled\n"
        f"- owner_review_steps: `{review_steps}`\n"
        "- policy_auto_apply: `false`\n"
        "- broker_action_allowed: `false`\n"
        "- production_effect: `none`\n"
    )


def render_weekly_cycle_report(
    manifest: Mapping[str, Any],
    steps: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic Rescue Confirmation Weekly Cycle",
        "",
        f"- weekly_cycle_id: `{manifest.get('weekly_cycle_id')}`",
        f"- week_ending: `{manifest.get('week_ending')}`",
        f"- dry_run: `{manifest.get('dry_run')}`",
        f"- due_windows: {summary.get('due_windows')}",
        f"- updated_windows: {summary.get('updated_windows')}",
        f"- ready_for_evaluation: {summary.get('ready_for_evaluation')}",
        f"- failure_count: {summary.get('failure_count')}",
        f"- owner_action_required: `{summary.get('owner_action_required')}`",
        f"- rule_review_recommendation: `{summary.get('rule_review_recommendation')}`",
        "- broker_action_allowed: `false`",
        "- production_effect: `none`",
        "",
        "## Steps",
    ]
    for row in steps:
        lines.append(
            f"- {_text(row.get('step'))}: `{row.get('status')}` artifact=`{row.get('artifact_id')}`"
        )
    return "\n".join(lines) + "\n"


def render_weekly_cycle_reader_brief(summary: Mapping[str, Any]) -> str:
    return (
        "## Dynamic Rescue Confirmation Weekly Cycle\n\n"
        f"- weekly_cycle_id: `{summary.get('weekly_cycle_id')}`\n"
        f"- due_windows: {summary.get('due_windows')}\n"
        f"- updated_windows: {summary.get('updated_windows')}\n"
        f"- ready_for_evaluation: {summary.get('ready_for_evaluation')}\n"
        f"- rule_review_recommendation: `{summary.get('rule_review_recommendation')}`\n"
        f"- owner_action_required: `{summary.get('owner_action_required')}`\n"
        "- production_effect: `none`\n"
    )


def render_pressure_regime_report(manifest: Mapping[str, Any], summary: Mapping[str, Any]) -> str:
    samples = _mapping(summary.get("pressure_samples"))
    return (
        "# Dynamic Rescue Pressure Regime Tagging\n\n"
        f"- tag_id: `{manifest.get('tag_id')}`\n"
        f"- date_range: `{manifest.get('start')}` to `{manifest.get('end')}`\n"
        f"- data_quality_status: `{manifest.get('data_quality_status')}`\n"
        f"- pressure_window_count: {summary.get('pressure_window_count')}\n"
        f"- tech_drawdown_count: {samples.get('tech_drawdown')}\n"
        f"- risk_off_count: {samples.get('risk_off')}\n"
        f"- semiconductor_pullback_count: {samples.get('semiconductor_pullback')}\n"
        f"- pressure_tagged_outcomes: {summary.get('pressure_tagged_outcomes')}\n"
        "- defensive_validation_relevant_outcomes: "
        f"{summary.get('defensive_validation_relevant_outcomes')}\n"
        f"- next_needed_samples: `{summary.get('next_needed_samples')}`\n"
        "- broker_action_allowed: `false`\n"
        "- production_effect: `none`\n"
    )


def render_confirmation_dashboard_report(
    manifest: Mapping[str, Any],
    target_table: Mapping[str, Any],
    pressure_dashboard: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic Rescue Confirmation Dashboard",
        "",
        f"- dashboard_id: `{manifest.get('dashboard_id')}`",
        f"- week_ending: `{manifest.get('week_ending')}`",
        f"- targets_total: {summary.get('targets_total')}",
        f"- ready_for_evaluation: {summary.get('ready_for_evaluation')}",
        f"- dashboard_recommendation: `{summary.get('dashboard_recommendation')}`",
        f"- owner_action_required: `{summary.get('owner_action_required')}`",
        "- policy_change_allowed: `false`",
        "- production_effect: `none`",
        "",
        "## Targets",
    ]
    for row in _records(target_table.get("targets")):
        lines.extend(
            [
                "",
                f"### {_text(row.get('target_id'))}",
                f"- status: `{row.get('status')}`",
                f"- available_forward_events: {row.get('available_forward_events')}",
                f"- required_forward_events: {row.get('required_forward_events')}",
                f"- available_pressure_events: {row.get('available_pressure_events')}",
                f"- required_pressure_events: {row.get('required_pressure_events')}",
                f"- progress_pct: {row.get('progress_pct')}",
                f"- decision: `{row.get('decision')}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Pressure Samples",
            "",
            f"- pressure_samples: `{pressure_dashboard.get('pressure_samples')}`",
            "- defensive_validation_status: "
            f"`{pressure_dashboard.get('defensive_validation_status')}`",
            f"- next_needed_samples: `{pressure_dashboard.get('next_needed_samples')}`",
        ]
    )
    return "\n".join(lines) + "\n"


def render_confirmation_dashboard_reader_brief(
    summary: Mapping[str, Any],
    target_table: Mapping[str, Any],
    pressure_dashboard: Mapping[str, Any],
) -> str:
    targets = {row["target_id"]: row for row in _records(target_table.get("targets"))}
    limited = _mapping(targets.get("limited_adjustment_vs_no_trade"))
    defensive = _mapping(targets.get("defensive_limited_adjustment_drawdown"))
    consensus = _mapping(targets.get("consensus_target_risk"))
    return (
        "## Dynamic Rescue Confirmation Dashboard\n\n"
        f"- targets_total: {summary.get('targets_total')}\n"
        f"- ready_for_evaluation: {summary.get('ready_for_evaluation')}\n"
        f"- limited_adjustment_progress: {limited.get('progress_pct')}\n"
        f"- defensive_pressure_sample_progress: {defensive.get('progress_pct')}\n"
        f"- consensus_target_status: `{consensus.get('status', 'MISSING')}`\n"
        f"- pressure_samples: `{pressure_dashboard.get('pressure_samples')}`\n"
        f"- dashboard_recommendation: `{summary.get('dashboard_recommendation')}`\n"
        "- production_effect: `none`\n"
    )


def render_rule_review_queue_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    items: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Dynamic Rescue Rule Review Queue",
        "",
        f"- queue_id: `{manifest.get('queue_id')}`",
        f"- source_cycle_id: `{manifest.get('source_cycle_id')}`",
        f"- pending_count: {summary.get('pending_count')}",
        f"- reviewed_count: {summary.get('reviewed_count')}",
        f"- deferred_count: {summary.get('deferred_count')}",
        f"- not_ready_count: {summary.get('not_ready_count')}",
        f"- ready_for_owner_review_count: {summary.get('ready_for_owner_review_count')}",
        f"- summary_recommendation: `{summary.get('summary_recommendation')}`",
        "- policy_change_allowed: `false`",
        "- broker_action_allowed: `false`",
        "- production_effect: `none`",
        "",
        "## Items",
    ]
    for row in items:
        lines.extend(
            [
                "",
                f"### {_text(row.get('item_id'))}",
                f"- target_id: `{row.get('target_id')}`",
                f"- queue_status: `{row.get('queue_status')}`",
                f"- recommended_owner_action: `{row.get('recommended_owner_action')}`",
                f"- evidence_status: `{row.get('evidence_status')}`",
                f"- summary: {row.get('summary')}",
            ]
        )
    return "\n".join(lines) + "\n"


def _read_yaml_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise DynamicV3ConfirmationOperationsError(f"config not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, Mapping):
        raise DynamicV3ConfirmationOperationsError(f"config root must be mapping: {path}")
    return dict(raw)


def _artifact_safety() -> dict[str, Any]:
    return {
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "auto_apply": False,
        "auto_policy_apply": False,
        "policy_change_allowed": False,
        "manual_review_required": True,
        "owner_approval_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _schedule_safety(config: Mapping[str, Any]) -> dict[str, Any]:
    safety = _mapping(config.get("safety"))
    return {
        "broker_action_allowed": safety.get("broker_action_allowed") is True and False,
        "broker_action_taken": False,
        "production_effect": "none",
        "auto_apply_policy": False,
        "owner_approval_required": safety.get("owner_approval_required") is not False,
    }


def _scheduled_commands(config_path: Path) -> list[dict[str, Any]]:
    config_arg = str(config_path).replace("\\", "/")
    rows = [
        (
            "outcome_due_scan",
            "aits etf dynamic-v3-rescue outcome-due scan --as-of <week_ending>",
            "review",
            False,
        ),
        (
            "outcome_update_review",
            "aits etf dynamic-v3-rescue outcome-update-review run --due-id <due_id>",
            "review",
            True,
        ),
        (
            "outcome_update_if_ready",
            "aits etf dynamic-v3-rescue outcome-update run --update-review-id <update_review_id>",
            "explicit_update_only",
            True,
        ),
        (
            "rolling_evidence_refresh",
            "aits etf dynamic-v3-rescue rolling-evidence-refresh run "
            "--outcome-update-id <outcome_update_id>",
            "post_update_review",
            False,
        ),
        (
            "confirmation_progress_update",
            "aits etf dynamic-v3-rescue confirmation-progress update --registry-id <registry_id>",
            "review",
            False,
        ),
        (
            "confirmation_evaluate",
            "aits etf dynamic-v3-rescue confirmation-evaluate run --progress-id <progress_id>",
            "review",
            False,
        ),
        (
            "rule_review_cycle",
            "aits etf dynamic-v3-rescue rule-review-cycle run "
            "--registry-id <registry_id> --progress-id <progress_id> "
            "--evaluation-id <evaluation_id>",
            "review",
            True,
        ),
        (
            "owner_decision_queue_update",
            "aits etf dynamic-v3-rescue rule-review-queue build",
            "review",
            True,
        ),
        (
            "weekly_dashboard",
            "aits etf dynamic-v3-rescue confirmation-dashboard build --week-ending <week_ending>",
            "review",
            False,
        ),
        (
            "reader_brief_update",
            "aits reports reader-brief --latest",
            "read_only",
            False,
        ),
    ]
    return [
        {
            "step": step,
            "command": command.replace(str(config_path), config_arg),
            "required": True,
            "execution_mode": mode,
            "owner_review_required": owner_review,
            "policy_change_allowed": False,
            "broker_action_allowed": False,
            "production_effect": "none",
        }
        for step, command, mode, owner_review in rows
    ]


def _step(step: str, status: str, artifact_id: str, **summary: Any) -> dict[str, Any]:
    row = {
        "step": step,
        "status": status,
        "artifact_id": artifact_id,
        "summary": summary,
        "production_effect": "none",
        "broker_action_allowed": False,
    }
    if "reason" in summary:
        row["reason"] = summary["reason"]
    return row


def _weekly_cycle_summary(
    *,
    week_ending: date,
    due_summary: Mapping[str, Any],
    update: Mapping[str, Any] | None,
    progress_summary: Mapping[str, Any],
    evaluation_summary: Mapping[str, Any],
    cycle_manifest: Mapping[str, Any],
    queue_summary: Mapping[str, Any],
) -> dict[str, Any]:
    updated = 0
    if update is not None:
        updated = len(_records(update.get("updated_windows")))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_cycle_summary",
        "weekly_cycle_id": "",
        "week_ending": week_ending.isoformat(),
        "due_windows": _int(due_summary.get("due_windows")),
        "updated_windows": updated,
        "forward_available": _int(progress_summary.get("available_forward_events")),
        "forward_pending": _int(due_summary.get("total_pending_windows")),
        "confirmation_targets_total": _int(progress_summary.get("targets_total")),
        "ready_for_evaluation": _int(progress_summary.get("ready_for_evaluation_count")),
        "success_count": _int(evaluation_summary.get("success_count")),
        "failure_count": _int(evaluation_summary.get("failure_count")),
        "not_ready_count": _int(evaluation_summary.get("not_ready_count")),
        "rule_review_recommendation": _text(
            cycle_manifest.get("cycle_recommendation"),
            "continue_tracking",
        ),
        "owner_action_required": _int(queue_summary.get("ready_for_owner_review_count")) > 0,
        "broker_action_allowed": False,
        "production_effect": "none",
        "auto_apply": False,
        "policy_change_allowed": False,
    }


def _pressure_quality_gate(
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
    from ai_trading_system.etf_portfolio.dynamic_v3_outcome_accumulation import (
        _quality_gate_for_cached_data,
    )

    return _quality_gate_for_cached_data(
        as_of=as_of,
        generated=generated,
        prices_path=prices_path,
        rates_path=rates_path,
        report_path=report_path,
        enforce=True,
    )


def _validate_pressure_config_or_raise(config: Mapping[str, Any]) -> None:
    validation = _pressure_config_checks(config)
    if not all(row["passed"] for row in validation):
        failed = ", ".join(row["check_id"] for row in validation if not row["passed"])
        raise DynamicV3ConfirmationOperationsError(f"pressure config validation failed: {failed}")


def validate_pressure_regime_tagging_config(
    *, config_path: Path = DEFAULT_PRESSURE_REGIME_TAGGING_CONFIG_PATH
) -> dict[str, Any]:
    config = _read_yaml_config(config_path)
    return _validation_payload(
        report_type="etf_dynamic_v3_pressure_regime_tagging_config_validation",
        artifact_id_key="config_path",
        artifact_id=str(config_path),
        checks=_pressure_config_checks(config),
    )


def _pressure_config_checks(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    thresholds = _mapping(config.get("thresholds"))
    windows = _mapping(config.get("windows"))
    symbols = _mapping(config.get("symbols"))
    return [
        _check("schema_version_supported", _int(config.get("schema_version")) == 1, "schema=1"),
        _check("tech_proxy_present", bool(_text(symbols.get("tech_proxy"))), "tech proxy"),
        _check(
            "semiconductor_proxy_present",
            bool(_text(symbols.get("semiconductor_proxy"))),
            "semiconductor proxy",
        ),
        _check(
            "thresholds_present",
            all(
                key in thresholds
                for key in (
                    "tech_drawdown_pct",
                    "semiconductor_pullback_pct",
                    "risk_off_volatility_percentile",
                    "strong_recovery_return_pct",
                    "sideways_trend_abs_max",
                )
            ),
            "threshold keys",
        ),
        _check(
            "rolling_windows_present",
            bool([_int(item) for item in windows.get("rolling_days", []) if _int(item) > 0]),
            "rolling days",
        ),
        _check(
            "review_metadata_present",
            bool(_mapping(config.get("policy_metadata")).get("owner"))
            and bool(_mapping(config.get("policy_metadata")).get("version")),
            "policy metadata",
        ),
    ]


def _load_price_frame(prices_path: Path, *, start: date, end: date) -> pd.DataFrame:
    if not prices_path.exists():
        return pd.DataFrame()
    frame = pd.read_csv(prices_path)
    symbol_col = "symbol" if "symbol" in frame.columns else "ticker"
    if "date" not in frame.columns or symbol_col not in frame.columns:
        return pd.DataFrame()
    price_col = "adj_close" if "adj_close" in frame.columns else "close"
    if price_col not in frame.columns:
        return pd.DataFrame()
    frame = frame.rename(columns={symbol_col: "symbol", price_col: "price"}).copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    frame["price"] = pd.to_numeric(frame["price"], errors="coerce")
    frame = frame.dropna(subset=["date", "symbol", "price"])
    return frame[(frame["date"] >= start) & (frame["date"] <= end)].sort_values(["symbol", "date"])


def _build_regime_window_tags(
    price_frame: pd.DataFrame, config: Mapping[str, Any]
) -> list[dict[str, Any]]:
    if price_frame.empty:
        return []
    symbols = _mapping(config.get("symbols"))
    thresholds = _mapping(config.get("thresholds"))
    windows = [_int(item) for item in _mapping(config.get("windows")).get("rolling_days", [])]
    tech_proxy = _text(symbols.get("tech_proxy"), "QQQ")
    semi_proxy = _text(symbols.get("semiconductor_proxy"), "SMH")
    fallback_semi = _text(symbols.get("fallback_semiconductor_proxy"), "SOXX")
    price_map = _symbol_price_map(price_frame)
    semi_symbol = semi_proxy if semi_proxy in price_map else fallback_semi
    base_rows: list[dict[str, Any]] = []
    for window in windows:
        tech_series = price_map.get(tech_proxy, [])
        for index in range(window - 1, len(tech_series)):
            slice_rows = tech_series[index - window + 1 : index + 1]
            start_date = slice_rows[0][0]
            end_date = slice_rows[-1][0]
            qqq_drawdown = _rolling_drawdown(slice_rows)
            qqq_return = _rolling_return(slice_rows)
            vol = _realized_volatility(slice_rows)
            semi_slice = _aligned_slice(price_map.get(semi_symbol, []), start_date, end_date)
            smh_drawdown = _rolling_drawdown(semi_slice)
            smh_return = _rolling_return(semi_slice)
            base_rows.append(
                {
                    "window": window,
                    "start_date": start_date,
                    "end_date": end_date,
                    "qqq_drawdown": qqq_drawdown,
                    "qqq_return": qqq_return,
                    "smh_drawdown": smh_drawdown,
                    "smh_return": smh_return,
                    "realized_volatility": vol,
                    "trend_slope": qqq_return / window if window else 0.0,
                }
            )
    vol_values = sorted(row["realized_volatility"] for row in base_rows)
    vol_threshold = _percentile(
        vol_values,
        _float(thresholds.get("risk_off_volatility_percentile")),
    )
    tagged = []
    for row in base_rows:
        tags = _regime_tags_for_metrics(row, thresholds, vol_threshold)
        tagged.append(
            {
                "window_id": _stable_id(
                    "pressure-window",
                    row["start_date"].isoformat(),
                    row["end_date"].isoformat(),
                    row["window"],
                ),
                "start_date": row["start_date"].isoformat(),
                "end_date": row["end_date"].isoformat(),
                "window_days": row["window"],
                "regime_tags": tags,
                "metrics": {
                    "qqq_drawdown": round(row["qqq_drawdown"], 6),
                    "smh_drawdown": round(row["smh_drawdown"], 6),
                    "realized_volatility": round(row["realized_volatility"], 6),
                    "trend_slope": round(row["trend_slope"], 6),
                    "qqq_return": round(row["qqq_return"], 6),
                    "smh_return": round(row["smh_return"], 6),
                },
                "tag_confidence": "HIGH" if tags else "LOW",
                "production_effect": "none",
                "broker_action_allowed": False,
            }
        )
    return tagged


def _pressure_regime_summary(
    *,
    window_tags: Sequence[Mapping[str, Any]],
    outcome_tags: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
    start: date,
    end: date,
) -> dict[str, Any]:
    sample_counts = Counter(
        tag for row in window_tags for tag in _records_to_texts(row.get("regime_tags"))
    )
    pressure_window_count = sum(
        1
        for row in window_tags
        if set(_records_to_texts(row.get("regime_tags"))) & PRESSURE_VALIDATION_TAGS
    )
    defensive_count = sum(
        1 for row in outcome_tags if row.get("defensive_validation_relevant") is True
    )
    required = _int(_mapping(config.get("validation")).get("required_pressure_events"), 5)
    next_needed = [
        tag
        for tag in ("tech_drawdown", "semiconductor_pullback", "risk_off")
        if sample_counts.get(tag, 0) <= 0
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_regime_summary",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "pressure_window_count": pressure_window_count,
        "pressure_samples": {tag: sample_counts.get(tag, 0) for tag in PRESSURE_TAGS},
        "pressure_tagged_outcomes": sum(
            1 for row in outcome_tags if row.get("pressure_regime") is True
        ),
        "defensive_validation_relevant_outcomes": defensive_count,
        "required_pressure_events": required,
        "defensive_validation_status": (
            "READY_FOR_REVIEW" if defensive_count >= required else "INSUFFICIENT_PRESSURE_EVENTS"
        ),
        "next_needed_samples": next_needed,
        "future_outcomes_to_watch": [
            row.get("outcome_id")
            for row in outcome_tags
            if row.get("tag_status") == "INSUFFICIENT_DATA"
        ][:10],
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _symbol_price_map(frame: pd.DataFrame) -> dict[str, list[tuple[date, float]]]:
    result: dict[str, list[tuple[date, float]]] = {}
    for symbol, group in frame.groupby("symbol"):
        result[str(symbol)] = [
            (row["date"], float(row["price"]))
            for row in group.sort_values("date").to_dict("records")
            if float(row["price"]) > 0
        ]
    return result


def _aligned_slice(
    series: Sequence[tuple[date, float]], start_date: date, end_date: date
) -> list[tuple[date, float]]:
    return [(day, price) for day, price in series if start_date <= day <= end_date]


def _rolling_return(rows: Sequence[tuple[date, float]]) -> float:
    if len(rows) < 2 or rows[0][1] == 0:
        return 0.0
    return rows[-1][1] / rows[0][1] - 1.0


def _rolling_drawdown(rows: Sequence[tuple[date, float]]) -> float:
    if not rows:
        return 0.0
    peak = max(price for _, price in rows)
    if peak <= 0:
        return 0.0
    return rows[-1][1] / peak - 1.0


def _realized_volatility(rows: Sequence[tuple[date, float]]) -> float:
    returns = []
    for (_, prior), (_, current) in zip(rows, rows[1:], strict=False):
        if prior > 0:
            returns.append(current / prior - 1.0)
    if len(returns) < 2:
        return 0.0
    series = pd.Series(returns)
    return float(series.std(ddof=0) * math.sqrt(252))


def _percentile(values: Sequence[float], percentile: float) -> float:
    if not values:
        return 0.0
    clipped = min(max(percentile, 0.0), 1.0)
    index = min(len(values) - 1, int(round((len(values) - 1) * clipped)))
    return values[index]


def _regime_tags_for_metrics(
    row: Mapping[str, Any], thresholds: Mapping[str, Any], vol_threshold: float
) -> list[str]:
    tags = []
    qqq_drawdown = _float(row.get("qqq_drawdown"))
    smh_drawdown = _float(row.get("smh_drawdown"))
    qqq_return = _float(row.get("qqq_return"))
    smh_return = _float(row.get("smh_return"))
    vol = _float(row.get("realized_volatility"))
    slope = _float(row.get("trend_slope"))
    tech_threshold = _float(thresholds.get("tech_drawdown_pct"))
    semi_threshold = _float(thresholds.get("semiconductor_pullback_pct"))
    if qqq_drawdown <= tech_threshold:
        tags.append("tech_drawdown")
    if smh_drawdown <= semi_threshold:
        tags.append("semiconductor_pullback")
    if qqq_drawdown <= tech_threshold and vol >= vol_threshold:
        tags.append("risk_off")
    if abs(slope) <= _float(thresholds.get("sideways_trend_abs_max")) and vol >= vol_threshold:
        tags.append("sideways_choppy")
    if qqq_return >= _float(thresholds.get("strong_recovery_return_pct")):
        tags.append("strong_recovery")
    if qqq_return > 0 and smh_return >= 0 and qqq_drawdown > tech_threshold:
        tags.append("ai_trend")
    return tags


def _dashboard_target_status_table(
    progress: Mapping[str, Any],
    evaluation: Mapping[str, Any],
    _pressure: Mapping[str, Any],
) -> dict[str, Any]:
    eval_by_target = {
        _text(row.get("target_id")): row for row in _records(evaluation.get("target_evaluations"))
    }
    targets = []
    for row in _records(progress.get("target_progress")):
        target_id = _text(row.get("target_id"))
        evaluation_row = _mapping(eval_by_target.get(target_id))
        required_forward = _int(row.get("required_forward_events"))
        required_pressure = _int(row.get("required_pressure_regime_events"))
        available_forward = _int(row.get("available_forward_events"))
        available_pressure = _int(row.get("available_pressure_regime_events"))
        required = required_pressure or required_forward
        available = available_pressure if required_pressure else available_forward
        progress_pct = round(available / required, 4) if required else 0.0
        targets.append(
            {
                "target_id": target_id,
                "status": _text(row.get("progress_status")),
                "target_status": _text(row.get("target_status")),
                "available_forward_events": available_forward,
                "required_forward_events": required_forward,
                "available_pressure_events": available_pressure,
                "required_pressure_events": required_pressure,
                "progress_pct": min(progress_pct, 1.0),
                "current_win_rate": _mapping(row.get("current_metrics")).get(
                    "win_rate_vs_no_trade"
                ),
                "current_avg_relative_return": _mapping(row.get("current_metrics")).get(
                    "avg_relative_return"
                ),
                "current_drawdown_delta": _mapping(row.get("current_metrics")).get(
                    "drawdown_delta"
                ),
                "decision": _text(evaluation_row.get("recommendation"), "continue_tracking"),
                "policy_change_allowed": False,
                "broker_action_allowed": False,
                "production_effect": "none",
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_target_status_table",
        "targets": targets,
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _pressure_sample_dashboard(pressure: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(pressure.get("pressure_regime_summary"))
    samples = _mapping(summary.get("pressure_samples"))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_sample_dashboard",
        "pressure_samples": {tag: _int(samples.get(tag)) for tag in PRESSURE_TAGS},
        "defensive_validation_status": _text(
            summary.get("defensive_validation_status"),
            "INSUFFICIENT_PRESSURE_EVENTS",
        ),
        "next_needed_samples": summary.get(
            "next_needed_samples",
            ["tech_drawdown", "semiconductor_pullback", "risk_off"],
        ),
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _confirmation_dashboard_summary(
    *,
    week_ending: date,
    target_table: Mapping[str, Any],
    rule_cycle: Mapping[str, Any],
    queue: Mapping[str, Any],
) -> dict[str, Any]:
    targets = _records(target_table.get("targets"))
    ready = sum(1 for row in targets if row.get("status") == "READY_FOR_EVALUATION")
    queue_summary = _mapping(queue.get("queue_summary"))
    recommendation = _text(rule_cycle.get("cycle_recommendation"), "continue_tracking")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_dashboard_summary",
        "week_ending": week_ending.isoformat(),
        "targets_total": len(targets),
        "ready_for_evaluation": ready,
        "continue_tracking": sum(
            1 for row in targets if row.get("decision") == "continue_tracking"
        ),
        "owner_action_required": _int(queue_summary.get("ready_for_owner_review_count")) > 0,
        "policy_change_allowed": False,
        "dashboard_recommendation": recommendation,
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _queue_item(
    row: Mapping[str, Any],
    decisions: Sequence[Mapping[str, Any]],
    cycle_payload: Mapping[str, Any],
) -> dict[str, Any]:
    target_id = _text(row.get("target_id"))
    reviewed = [
        item
        for item in decisions
        if _text(item.get("cycle_id")) == _text(cycle_payload.get("cycle_id"))
        and target_id in _records_to_texts(item.get("target_ids"))
        and _text(item.get("owner_decision")) not in {"", "pending"}
    ]
    decision = _text(row.get("rule_review_decision"))
    owner_action = row.get("owner_action_required") is True
    if reviewed:
        status = "reviewed"
    elif owner_action:
        status = "pending"
    elif decision == "DEFER":
        status = "deferred"
    else:
        status = "not_ready"
    evidence = "READY_FOR_REVIEW" if owner_action else "NOT_READY"
    recommended = _recommended_owner_action(decision, status)
    return {
        "item_id": _stable_id(
            "rule-review-queue-item",
            _text(cycle_payload.get("cycle_id")),
            target_id,
        ),
        "target_id": target_id,
        "source_cycle_id": _text(cycle_payload.get("cycle_id")),
        "source_evaluation_id": _text(cycle_payload.get("evaluation_id")),
        "owner_decision_id": _text(reviewed[-1].get("decision_id")) if reviewed else "",
        "owner_decision": _text(reviewed[-1].get("owner_decision")) if reviewed else "",
        "queue_status": status,
        "recommended_owner_action": recommended,
        "evidence_status": evidence,
        "policy_change_allowed": False,
        "auto_apply": False,
        "broker_action_allowed": False,
        "summary": _text(row.get("reason")),
        "production_effect": "none",
    }


def _recommended_owner_action(decision: str, status: str) -> str:
    if status == "reviewed":
        return "continue_tracking"
    if decision in {
        "READY_FOR_OWNER_REVIEW",
        "TIGHTEN_RULES_RECOMMENDED",
        "LOOSEN_RULES_RECOMMENDED",
        "RENAME_OR_RECLASSIFY",
    }:
        return "manual_policy_review"
    if decision == "KEEP_REFERENCE_ONLY":
        return "keep_reference_only"
    if decision == "DEFER":
        return "request_more_data"
    return "continue_tracking"


def _queue_summary(items: Sequence[Mapping[str, Any]], generated: datetime) -> dict[str, Any]:
    counts = Counter(_text(row.get("queue_status")) for row in items)
    ready = sum(1 for row in items if row.get("evidence_status") == "READY_FOR_REVIEW")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_rule_review_queue_summary",
        "queue_id": "",
        "generated_at": generated.isoformat(),
        "pending_count": counts.get("pending", 0),
        "reviewed_count": counts.get("reviewed", 0),
        "deferred_count": counts.get("deferred", 0),
        "not_ready_count": counts.get("not_ready", 0),
        "ready_for_owner_review_count": ready,
        "summary_recommendation": "owner_review_required" if ready else "continue_tracking",
        "policy_change_allowed": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _records_to_texts(value: Any) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        return []
    return [_text(item) for item in value if _text(item)]


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""
