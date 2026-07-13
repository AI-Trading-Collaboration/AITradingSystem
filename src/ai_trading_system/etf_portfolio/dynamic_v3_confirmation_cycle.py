from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DEFAULT_FORWARD_CONFIRMATION_PLAN_DIR,
    _datetime_from_any,
    validate_forward_confirmation_plan_artifact,
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
    _write_json,
    _write_jsonl,
    _write_text,
)
from ai_trading_system.etf_portfolio.dynamic_v3_outcome_accumulation import (
    DEFAULT_CONSENSUS_RISK_DIR,
    DEFAULT_LIMITED_VS_NOTRADE_DIR,
    validate_consensus_risk_artifact,
    validate_limited_vs_notrade_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
    DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    SCHEMA_VERSION,
)
from ai_trading_system.platform.artifacts.writer import (
    write_json_atomic,
    write_text_atomic,
    write_yaml_atomic,
)

DEFAULT_CONFIRMATION_REGISTRY_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "forward_confirmation_registry"
)
DEFAULT_CONFIRMATION_REGISTRY_YAML_PATH = (
    PROJECT_ROOT
    / "registry"
    / "etf_portfolio"
    / "dynamic_v3_rescue_forward_confirmation_targets.yaml"
)
DEFAULT_CONFIRMATION_PROGRESS_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "confirmation_progress"
DEFAULT_CONFIRMATION_EVALUATION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "confirmation_evaluation"
DEFAULT_RULE_REVIEW_CYCLE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "rule_review_cycle"
DEFAULT_RULE_OWNER_DECISION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "rule_owner_decision"
DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH = (
    DEFAULT_RULE_OWNER_DECISION_DIR / "rule_owner_decision_journal.jsonl"
)
CONFIRMATION_REGISTRY_SNAPSHOT_SCHEMA_VERSION = "confirmation_registry_input_snapshot.v2"
CONFIRMATION_PROGRESS_SNAPSHOT_SCHEMA_VERSION = "confirmation_progress_input_snapshot.v2"
CONFIRMATION_EVALUATION_SNAPSHOT_SCHEMA_VERSION = "confirmation_evaluation_input_snapshot.v2"
RULE_REVIEW_CYCLE_SNAPSHOT_SCHEMA_VERSION = "rule_review_cycle_input_snapshot.v2"
RULE_OWNER_DECISION_SOURCE_SNAPSHOT_SCHEMA_VERSION = (
    "rule_owner_decision_source_snapshot.v2"
)
RULE_OWNER_DECISION_EVENT_SCHEMA_VERSION = "rule_owner_decision_event.v2"
RULE_OWNER_DECISION_RECORD_SCHEMA_VERSION = "rule_owner_decision_record.v2"
RULE_OWNER_DECISION_MANIFEST_SCHEMA_VERSION = "rule_owner_decision_manifest.v2"

# Source Plan conditions are declarative labels. Evaluation binds each label to the
# corresponding source criterion so no independent threshold is introduced here.
FAILURE_CONDITION_CRITERION_KEYS: dict[str, tuple[str, ...]] = {
    "underperforms_no_trade": ("avg_relative_return_min",),
    "drawdown_worsening_persists": ("avg_drawdown_delta_max", "drawdown_delta_max"),
    "fails_to_reduce_drawdown_in_pressure_regime": ("drawdown_delta_vs_no_trade_max",),
    "excess_drawdown_persists": ("drawdown_delta_vs_limited_adjustment_max",),
}

PROGRESS_STATUSES = {
    "INSUFFICIENT_EVENTS",
    "IN_PROGRESS",
    "NEAR_READY",
    "READY_FOR_EVALUATION",
    "BLOCKED",
}
EVALUATION_STATUSES = {"NOT_READY", "SUCCESS", "FAILURE", "MIXED", "REVIEW_REQUIRED"}
RULE_REVIEW_DECISIONS = {
    "CONTINUE_TRACKING",
    "READY_FOR_OWNER_REVIEW",
    "KEEP_CURRENT_RULES",
    "TIGHTEN_RULES_RECOMMENDED",
    "LOOSEN_RULES_RECOMMENDED",
    "KEEP_REFERENCE_ONLY",
    "RENAME_OR_RECLASSIFY",
    "DEFER",
}
OWNER_DECISIONS = {
    "pending",
    "continue_tracking",
    "keep_current_rules",
    "request_more_data",
    "approve_manual_policy_review",
    "reject_rule_change",
    "defer",
}
FINAL_OWNER_DECISIONS = (
    "continue_tracking",
    "keep_current_rules",
    "request_more_data",
    "approve_manual_policy_review",
    "reject_rule_change",
    "defer",
)


class DynamicV3ConfirmationCycleError(ValueError):
    """Raised when forward confirmation cycle inputs or artifacts are invalid."""


def register_confirmation_targets(
    *,
    confirmation_plan_id: str,
    confirmation_plan_dir: Path = DEFAULT_FORWARD_CONFIRMATION_PLAN_DIR,
    output_dir: Path = DEFAULT_CONFIRMATION_REGISTRY_DIR,
    registry_yaml_path: Path = DEFAULT_CONFIRMATION_REGISTRY_YAML_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    if generated.tzinfo is None or generated.utcoffset() is None:
        raise DynamicV3ConfirmationCycleError("generated_at must be timezone-aware")
    generated = generated.astimezone(UTC)
    plan_dir = confirmation_plan_dir / confirmation_plan_id
    plan_validation = validate_forward_confirmation_plan_artifact(
        confirmation_plan_id=confirmation_plan_id, output_dir=confirmation_plan_dir
    )
    if plan_validation.get("status") != "PASS":
        raise DynamicV3ConfirmationCycleError("confirmation plan validation failed")
    plan_bundle = _confirmation_registry_plan_bundle(plan_dir)
    plan_json = _mapping(plan_bundle.get("json"))
    plan_manifest = _mapping(plan_json.get("confirmation_plan_manifest.json"))
    if plan_manifest.get("status") != "AVAILABLE":
        raise DynamicV3ConfirmationCycleError("confirmation plan is not AVAILABLE")
    plan_generated = _datetime_from_any(plan_manifest.get("generated_at"))
    if plan_generated is None or plan_generated > generated:
        raise DynamicV3ConfirmationCycleError("confirmation plan generated after registry cutoff")
    plan_targets = _mapping(plan_json.get("confirmation_targets.json"))
    plan_failures = _mapping(plan_json.get("failure_conditions.json"))
    targets = _registry_targets_from_plan(plan_targets, plan_failures)
    preimage_text = (
        registry_yaml_path.read_text(encoding="utf-8") if registry_yaml_path.is_file() else ""
    )
    preimage = _read_yaml_optional(registry_yaml_path)
    if preimage.get("source_confirmation_plan_id") == confirmation_plan_id:
        raise DynamicV3ConfirmationCycleError("confirmation plan already registered")
    registry_id = _stable_id("forward-confirmation-registry", confirmation_plan_id, generated)
    registry_dir = _unique_dir(output_dir / registry_id)
    registry = {
        "schema_version": SCHEMA_VERSION,
        "source_confirmation_plan_id": confirmation_plan_id,
        "created_at": generated.isoformat(),
        "status": "active",
        "targets": targets,
        "production_effect": "none",
        "broker_action_allowed": False,
        "auto_apply": False,
        "owner_approval_required": True,
    }
    materialized_registry = {**registry, "registry_id": registry_dir.name}
    input_snapshot = {
        "schema_version": CONFIRMATION_REGISTRY_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_registry_input_snapshot",
        "generated_at": generated.isoformat(),
        "confirmation_plan_dir": str(plan_dir),
        "confirmation_plan_bundle": plan_bundle,
        "confirmation_plan_validation": plan_validation,
        "registry_yaml_path": str(registry_yaml_path),
        "registry_preimage": {
            "exists": bool(preimage_text),
            "payload": preimage,
            "file_contents": preimage_text,
        },
        "materialized_registry": materialized_registry,
        "lineage": {"confirmation_plan_id": confirmation_plan_id},
        "production_effect": "none",
    }
    manifest = _confirmation_registry_manifest(
        registry_dir=registry_dir,
        confirmation_plan_id=confirmation_plan_id,
        plan_manifest=plan_manifest,
        generated=generated,
        targets=targets,
        registry_yaml_path=registry_yaml_path,
    )
    report = render_confirmation_registry_report(manifest, registry)
    registry_dir.mkdir(parents=True, exist_ok=False)
    _write_json(registry_dir / "confirmation_registry_manifest.json", manifest)
    write_yaml_atomic(registry_dir / "registered_targets.yaml", registry, sort_keys=False)
    _write_json(registry_dir / "confirmation_registry_input_snapshot.json", input_snapshot)
    _write_text(registry_dir / "confirmation_targets_report.md", report)
    write_yaml_atomic(registry_yaml_path, materialized_registry, sort_keys=False)
    _update_latest_pointer(
        "latest_forward_confirmation_registry",
        registry_dir.name,
        registry_dir / "confirmation_registry_manifest.json",
    )
    return {
        "registry_id": registry_dir.name,
        "registry_dir": registry_dir,
        "manifest": manifest,
        "registry": registry,
        "targets": targets,
        "input_snapshot": input_snapshot,
    }


def _confirmation_registry_manifest(
    *,
    registry_dir: Path,
    confirmation_plan_id: str,
    plan_manifest: Mapping[str, Any],
    generated: datetime,
    targets: Sequence[Mapping[str, Any]],
    registry_yaml_path: Path,
) -> dict[str, Any]:
    active_count = sum(1 for row in targets if row.get("status") == "active")
    watch_only_count = sum(1 for row in targets if row.get("status") == "watch_only")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_forward_confirmation_registry_manifest",
        "registry_id": registry_dir.name,
        "source_confirmation_plan_id": confirmation_plan_id,
        "source_confirmation_plan_status": _text(plan_manifest.get("status")),
        "generated_at": generated.isoformat(),
        "status": "PASS" if targets else "FAIL",
        "targets_total": len(targets),
        "active_target_count": active_count,
        "watch_only_target_count": watch_only_count,
        "confirmation_registry_manifest_path": str(
            registry_dir / "confirmation_registry_manifest.json"
        ),
        "registered_targets_path": str(registry_dir / "registered_targets.yaml"),
        "confirmation_registry_input_snapshot_path": str(
            registry_dir / "confirmation_registry_input_snapshot.json"
        ),
        "confirmation_targets_report_path": str(registry_dir / "confirmation_targets_report.md"),
        "registry_yaml_path": str(registry_yaml_path),
        "market_regime": "ai_after_chatgpt",
        "auto_apply": False,
        "owner_approval_required": True,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _confirmation_registry_plan_bundle(plan_dir: Path) -> dict[str, Any]:
    json_files = (
        "confirmation_plan_manifest.json",
        "confirmation_targets.json",
        "trigger_conditions.json",
        "failure_conditions.json",
        "forward_confirmation_plan_input_snapshot.json",
    )
    text_files = ("forward_confirmation_plan_report.md", "reader_brief_section.md")
    return _bounded_source_bundle(
        source_dir=plan_dir,
        canonical_files=json_files + text_files,
        json_views=tuple(
            name for name in json_files if name != "forward_confirmation_plan_input_snapshot.json"
        ),
    )


def _registry_targets_from_plan(
    plan_targets: Mapping[str, Any], failure_conditions: Mapping[str, Any]
) -> list[dict[str, Any]]:
    source_rows = _records(plan_targets.get("targets"))
    target_ids = [_text(row.get("target_id")) for row in source_rows]
    if (
        not source_rows
        or any(not item for item in target_ids)
        or len(target_ids) != len(set(target_ids))
    ):
        raise DynamicV3ConfirmationCycleError("plan targets missing or duplicate")
    failures = _records(failure_conditions.get("failure_conditions"))
    failure_by_target: dict[str, list[dict[str, Any]]] = {}
    for row in failures:
        target_id = _text(row.get("target"))
        if (
            target_id not in target_ids
            or not _text(row.get("condition"))
            or not _text(row.get("action"))
        ):
            raise DynamicV3ConfirmationCycleError("plan failure condition invalid")
        failure_by_target.setdefault(target_id, []).append(dict(row))
    targets = []
    for row in source_rows:
        target_id = _text(row.get("target_id"))
        if row.get("current_status") != "TRACKING_REQUIRED":
            raise DynamicV3ConfirmationCycleError(f"unsupported plan target status: {target_id}")
        if not failure_by_target.get(target_id):
            raise DynamicV3ConfirmationCycleError(f"missing failure condition: {target_id}")
        targets.append(
            {
                "target_id": target_id,
                "status": "active",
                "priority": row.get("priority"),
                "source": "backtest_sim_forward_confirmation_plan",
                "windows": list(row.get("windows", [])),
                "required_forward_events": row.get("required_forward_events"),
                "success_criteria": dict(_mapping(row.get("success_criteria"))),
                "failure_conditions": failure_by_target[target_id],
                "current_status": "in_progress",
                "source_plan_target": dict(row),
                "owner_approval_required": True,
                "auto_apply": False,
                "broker_action_allowed": False,
                "production_effect": "none",
            }
        )
    return targets


def confirmation_targets_report_payload(
    *,
    registry_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CONFIRMATION_REGISTRY_DIR,
) -> dict[str, Any]:
    registry_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=registry_id if not latest else None,
        pointer_name="latest_forward_confirmation_registry",
    )
    return {
        **_read_json(registry_dir / "confirmation_registry_manifest.json"),
        "registered_targets": _read_yaml(registry_dir / "registered_targets.yaml"),
        "confirmation_targets_report": _read_text(registry_dir / "confirmation_targets_report.md"),
        "registry_dir": str(registry_dir),
    }


def list_confirmation_targets(
    *,
    registry_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CONFIRMATION_REGISTRY_DIR,
) -> dict[str, Any]:
    payload = confirmation_targets_report_payload(
        registry_id=registry_id,
        latest=latest or registry_id is None,
        output_dir=output_dir,
    )
    registry = _mapping(payload.get("registered_targets"))
    targets = _records(registry.get("targets"))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_forward_confirmation_target_list",
        "registry_id": payload.get("registry_id"),
        "targets_total": len(targets),
        "active_target_count": sum(1 for row in targets if row.get("status") == "active"),
        "watch_only_target_count": sum(1 for row in targets if row.get("status") == "watch_only"),
        "targets": targets,
        "production_effect": "none",
        "broker_action_allowed": False,
        "auto_apply": False,
        "owner_approval_required": True,
    }


def validate_confirmation_targets_artifact(
    *,
    registry_id: str,
    output_dir: Path = DEFAULT_CONFIRMATION_REGISTRY_DIR,
) -> dict[str, Any]:
    registry_dir = output_dir / registry_id
    manifest = _read_optional_json(registry_dir / "confirmation_registry_manifest.json") or {}
    registry = _read_yaml_optional(registry_dir / "registered_targets.yaml")
    snapshot = _read_optional_json(registry_dir / "confirmation_registry_input_snapshot.json") or {}
    targets = _records(registry.get("targets"))
    target_ids = [_text(row.get("target_id")) for row in targets]
    source_errors: list[str] = []
    recompute_error = ""
    expected_registry: dict[str, Any] = {}
    expected_snapshot: dict[str, Any] = {}
    expected_manifest: dict[str, Any] = {}
    expected_materialized: dict[str, Any] = {}
    expected_report = ""
    try:
        if snapshot.get("schema_version") != CONFIRMATION_REGISTRY_SNAPSHOT_SCHEMA_VERSION:
            source_errors.append("registry snapshot schema mismatch")
        generated = _datetime_from_any(snapshot.get("generated_at"))
        plan_dir = Path(_text(snapshot.get("confirmation_plan_dir")))
        plan_id = _text(_mapping(snapshot.get("lineage")).get("confirmation_plan_id"))
        if generated is None or plan_dir.name != plan_id:
            raise DynamicV3ConfirmationCycleError("registry snapshot identity/time invalid")
        live_validation = validate_forward_confirmation_plan_artifact(
            confirmation_plan_id=plan_id, output_dir=plan_dir.parent
        )
        if live_validation.get("status") != "PASS":
            source_errors.append("confirmation plan validation failed")
        if live_validation != snapshot.get("confirmation_plan_validation"):
            source_errors.append("confirmation plan validation changed")
        live_bundle = _confirmation_registry_plan_bundle(plan_dir)
        if live_bundle != snapshot.get("confirmation_plan_bundle"):
            source_errors.append("confirmation plan bundle changed")
        plan_json = _mapping(live_bundle.get("json"))
        plan_manifest = _mapping(plan_json.get("confirmation_plan_manifest.json"))
        if plan_manifest.get("status") != "AVAILABLE":
            source_errors.append("confirmation plan not available")
        plan_generated = _datetime_from_any(plan_manifest.get("generated_at"))
        if plan_generated is None or plan_generated > generated:
            source_errors.append("confirmation plan after cutoff")
        expected_targets = _registry_targets_from_plan(
            _mapping(plan_json.get("confirmation_targets.json")),
            _mapping(plan_json.get("failure_conditions.json")),
        )
        expected_registry = {
            "schema_version": SCHEMA_VERSION,
            "source_confirmation_plan_id": plan_id,
            "created_at": generated.isoformat(),
            "status": "active",
            "targets": expected_targets,
            "production_effect": "none",
            "broker_action_allowed": False,
            "auto_apply": False,
            "owner_approval_required": True,
        }
        registry_yaml_path = Path(_text(snapshot.get("registry_yaml_path")))
        expected_materialized = {**expected_registry, "registry_id": registry_id}
        expected_snapshot = {
            "schema_version": CONFIRMATION_REGISTRY_SNAPSHOT_SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_confirmation_registry_input_snapshot",
            "generated_at": generated.isoformat(),
            "confirmation_plan_dir": str(plan_dir),
            "confirmation_plan_bundle": live_bundle,
            "confirmation_plan_validation": live_validation,
            "registry_yaml_path": str(registry_yaml_path),
            "registry_preimage": snapshot.get("registry_preimage"),
            "materialized_registry": expected_materialized,
            "lineage": {"confirmation_plan_id": plan_id},
            "production_effect": "none",
        }
        expected_manifest = _confirmation_registry_manifest(
            registry_dir=registry_dir,
            confirmation_plan_id=plan_id,
            plan_manifest=plan_manifest,
            generated=generated,
            targets=expected_targets,
            registry_yaml_path=registry_yaml_path,
        )
        expected_report = render_confirmation_registry_report(expected_manifest, expected_registry)
        materialized = _read_yaml_optional(registry_yaml_path)
        if materialized.get("registry_id") == registry_id and materialized != expected_materialized:
            source_errors.append("current materialized registry changed")
    except Exception as exc:  # noqa: BLE001
        recompute_error = str(exc)
    checks = [
        _check(
            "manifest_exists",
            (registry_dir / "confirmation_registry_manifest.json").exists(),
            "",
        ),
        _check("targets_yaml_exists", (registry_dir / "registered_targets.yaml").exists(), ""),
        _check("report_exists", (registry_dir / "confirmation_targets_report.md").exists(), ""),
        _check(
            "snapshot_exists",
            (registry_dir / "confirmation_registry_input_snapshot.json").exists(),
            "",
        ),
        _check("registry_id_matches", manifest.get("registry_id") == registry_id, ""),
        _check("target_ids_unique", len(target_ids) == len(set(target_ids)), "target_id"),
        _check("target_count_nonzero", len(targets) > 0, "targets"),
        _check("source_snapshot_valid", not source_errors, ",".join(source_errors)),
        _check("views_recomputed", not recompute_error, recompute_error),
        _check("registry_recomputed", registry == expected_registry, "validated plan"),
        _check("snapshot_recomputed", snapshot == expected_snapshot, "validated plan"),
        _check("manifest_recomputed", manifest == expected_manifest, "snapshot"),
        _check(
            "json_bytes_recomputed",
            all(
                path.is_file()
                and path.read_text(encoding="utf-8")
                == json.dumps(_jsonable(payload), ensure_ascii=False, indent=2, sort_keys=True)
                + "\n"
                for path, payload in (
                    (registry_dir / "confirmation_registry_manifest.json", expected_manifest),
                    (registry_dir / "confirmation_registry_input_snapshot.json", expected_snapshot),
                )
            ),
            "canonical JSON bytes",
        ),
        _check(
            "registry_yaml_bytes_recomputed",
            (registry_dir / "registered_targets.yaml").is_file()
            and _read_text(registry_dir / "registered_targets.yaml")
            == yaml.safe_dump(_jsonable(expected_registry), sort_keys=False, allow_unicode=True),
            "canonical registry YAML bytes",
        ),
        _check(
            "report_recomputed",
            (registry_dir / "confirmation_targets_report.md").is_file()
            and _read_text(registry_dir / "confirmation_targets_report.md") == expected_report,
            "report bytes",
        ),
        _check(
            "auto_apply_forbidden",
            manifest.get("auto_apply") is False
            and all(row.get("auto_apply") is False for row in targets),
            "auto_apply=false",
        ),
        _check(
            "owner_approval_required",
            manifest.get("owner_approval_required") is True
            and all(row.get("owner_approval_required") is True for row in targets),
            "owner approval",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_forward_confirmation_registry_validation",
        artifact_id_key="registry_id",
        artifact_id=registry_id,
        checks=checks,
    )


def _confirmation_progress_registry_bundle(registry_dir: Path) -> dict[str, Any]:
    json_files = (
        "confirmation_registry_manifest.json",
        "confirmation_registry_input_snapshot.json",
    )
    yaml_files = ("registered_targets.yaml",)
    text_files = ("confirmation_targets_report.md",)
    return _bounded_source_bundle(
        source_dir=registry_dir,
        canonical_files=json_files + yaml_files + text_files,
        json_views=("confirmation_registry_manifest.json",),
        yaml_views=yaml_files,
    )


def _progress_source_kind(target_id: str) -> str:
    mapping = {
        "limited_adjustment_vs_no_trade": "limited_vs_notrade",
        "defensive_limited_adjustment_drawdown": "limited_vs_notrade",
        "consensus_target_risk": "consensus_risk",
    }
    if target_id not in mapping:
        raise DynamicV3ConfirmationCycleError(f"unsupported confirmation target: {target_id}")
    return mapping[target_id]


def _confirmation_progress_source_bundle(*, kind: str, source_dir: Path) -> dict[str, Any]:
    if kind == "limited_vs_notrade":
        json_files = (
            "limited_vs_notrade_manifest.json",
            "window_comparison_metrics.json",
            "regime_breakdown.json",
            "limited_vs_notrade_source_snapshot.json",
        )
        jsonl_files = ("sample_inventory.jsonl",)
        text_files = ("limited_vs_notrade_report.md",)
    elif kind == "consensus_risk":
        json_files = (
            "consensus_risk_manifest.json",
            "consensus_exposure_summary.json",
            "consensus_drawdown_risk.json",
            "consensus_turnover_risk.json",
            "consensus_risk_source_snapshot.json",
        )
        jsonl_files = ("consensus_exposure_samples.jsonl", "consensus_drawdown_pairs.jsonl")
        text_files = ("consensus_risk_report.md",)
    else:
        raise DynamicV3ConfirmationCycleError(f"unsupported progress source kind: {kind}")
    return _bounded_source_bundle(
        source_dir=source_dir,
        canonical_files=json_files + jsonl_files + text_files,
        json_views=tuple(name for name in json_files if not name.endswith("_source_snapshot.json")),
        jsonl_views=jsonl_files,
    )


def _select_confirmation_progress_source(
    *, kind: str, output_dir: Path, generated: datetime
) -> dict[str, Any]:
    manifest_name = (
        "limited_vs_notrade_manifest.json"
        if kind == "limited_vs_notrade"
        else "consensus_risk_manifest.json"
    )
    id_key = "focus_id" if kind == "limited_vs_notrade" else "risk_id"
    candidates: list[tuple[datetime, str, Path]] = []
    if output_dir.is_dir():
        for child in output_dir.iterdir():
            if not child.is_dir() or not (child / manifest_name).is_file():
                continue
            manifest = _read_json(child / manifest_name)
            artifact_id = _text(manifest.get(id_key))
            source_generated = _datetime_from_any(manifest.get("generated_at"))
            if not artifact_id or artifact_id != child.name or source_generated is None:
                raise DynamicV3ConfirmationCycleError(f"{kind} source identity/time invalid")
            if source_generated <= generated:
                candidates.append((source_generated, artifact_id, child))
    if not candidates:
        return {
            "source_kind": kind,
            "selection_status": "MISSING",
            "artifact_id": None,
            "source_root": str(output_dir),
            "source_dir": None,
            "validation": None,
            "bundle": None,
        }
    identities = [(item[0].isoformat(), item[1]) for item in candidates]
    if len(identities) != len(set(identities)):
        raise DynamicV3ConfirmationCycleError(f"duplicate {kind} source identity")
    _, artifact_id, source_dir = max(candidates, key=lambda item: (item[0], item[1]))
    validation = (
        validate_limited_vs_notrade_artifact(focus_id=artifact_id, output_dir=output_dir)
        if kind == "limited_vs_notrade"
        else validate_consensus_risk_artifact(risk_id=artifact_id, output_dir=output_dir)
    )
    if validation.get("status") != "PASS":
        raise DynamicV3ConfirmationCycleError(f"{kind} source validation failed")
    return {
        "source_kind": kind,
        "selection_status": "SELECTED",
        "artifact_id": artifact_id,
        "source_root": str(output_dir),
        "source_dir": str(source_dir),
        "validation": validation,
        "bundle": _confirmation_progress_source_bundle(kind=kind, source_dir=source_dir),
    }


def _confirmation_progress_rows_from_snapshot(
    snapshot: Mapping[str, Any],
) -> list[dict[str, Any]]:
    generated = _datetime_from_any(snapshot.get("generated_at"))
    if generated is None:
        raise DynamicV3ConfirmationCycleError("progress snapshot generated_at invalid")
    registry_bundle = _mapping(snapshot.get("registry_bundle"))
    registry = _mapping(_mapping(registry_bundle.get("yaml")).get("registered_targets.yaml"))
    sources = _mapping(snapshot.get("evidence_sources"))
    rows = []
    for target in _records(registry.get("targets")):
        kind = _progress_source_kind(_text(target.get("target_id")))
        source = _mapping(sources.get(kind))
        if source.get("selection_status") not in {"SELECTED", "MISSING"}:
            raise DynamicV3ConfirmationCycleError(f"{kind} selection status invalid")
        bundle = (
            _mapping(source.get("bundle")) if source.get("selection_status") == "SELECTED" else {}
        )
        rows.append(
            _progress_row_v2(
                target,
                source_kind=kind,
                source_bundle=bundle,
                source_missing=source.get("selection_status") == "MISSING",
                generated=generated,
            )
        )
    return rows


def _confirmation_progress_manifest(
    *,
    progress_dir: Path,
    registry_id: str,
    generated: datetime,
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_progress_manifest",
        "progress_id": progress_dir.name,
        "registry_id": registry_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if rows else "FAIL",
        "targets_total": len(rows),
        "ready_for_evaluation_count": summary["ready_for_evaluation_count"],
        "insufficient_events_count": summary["insufficient_events_count"],
        "confirmation_progress_manifest_path": str(
            progress_dir / "confirmation_progress_manifest.json"
        ),
        "target_progress_path": str(progress_dir / "target_progress.jsonl"),
        "target_progress_summary_path": str(progress_dir / "target_progress_summary.json"),
        "confirmation_progress_report_path": str(progress_dir / "confirmation_progress_report.md"),
        "confirmation_progress_input_snapshot_path": str(
            progress_dir / "confirmation_progress_input_snapshot.json"
        ),
        "market_regime": "ai_after_chatgpt",
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "auto_apply": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def update_confirmation_progress(
    *,
    registry_id: str,
    registry_dir: Path = DEFAULT_CONFIRMATION_REGISTRY_DIR,
    output_dir: Path = DEFAULT_CONFIRMATION_PROGRESS_DIR,
    limited_vs_notrade_dir: Path = DEFAULT_LIMITED_VS_NOTRADE_DIR,
    consensus_risk_dir: Path = DEFAULT_CONSENSUS_RISK_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    if generated.tzinfo is None or generated.utcoffset() is None:
        raise DynamicV3ConfirmationCycleError("generated_at must be timezone-aware")
    generated = generated.astimezone(UTC)
    registry_validation = validate_confirmation_targets_artifact(
        registry_id=registry_id, output_dir=registry_dir
    )
    if registry_validation.get("status") != "PASS":
        raise DynamicV3ConfirmationCycleError("confirmation registry validation failed")
    registry_artifact_dir = registry_dir / registry_id
    registry_bundle = _confirmation_progress_registry_bundle(registry_artifact_dir)
    registry_manifest = _mapping(
        _mapping(registry_bundle.get("json")).get("confirmation_registry_manifest.json")
    )
    registry_generated = _datetime_from_any(registry_manifest.get("generated_at"))
    if registry_generated is None or registry_generated > generated:
        raise DynamicV3ConfirmationCycleError("confirmation registry generated after cutoff")
    registry = _mapping(_mapping(registry_bundle.get("yaml")).get("registered_targets.yaml"))
    targets = _records(registry.get("targets"))
    source_kinds = {_progress_source_kind(_text(row.get("target_id"))) for row in targets}
    evidence_sources = {
        kind: _select_confirmation_progress_source(
            kind=kind,
            output_dir=(
                limited_vs_notrade_dir if kind == "limited_vs_notrade" else consensus_risk_dir
            ),
            generated=generated,
        )
        for kind in sorted(source_kinds)
    }
    snapshot = {
        "schema_version": CONFIRMATION_PROGRESS_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_progress_input_snapshot",
        "generated_at": generated.isoformat(),
        "registry_id": registry_id,
        "registry_root": str(registry_dir),
        "registry_bundle": registry_bundle,
        "registry_validation": registry_validation,
        "evidence_sources": evidence_sources,
        "selection_inventory": [
            {
                "source_kind": kind,
                "selection_status": _mapping(source).get("selection_status"),
                "artifact_id": _mapping(source).get("artifact_id"),
                "source_dir": _mapping(source).get("source_dir"),
            }
            for kind, source in sorted(evidence_sources.items())
        ],
        "production_effect": "none",
    }
    rows = _confirmation_progress_rows_from_snapshot(snapshot)
    summary = _progress_summary(rows, registry_id, generated)
    progress_id = _stable_id("confirmation-progress", registry_id, generated)
    progress_dir = _unique_dir(output_dir / progress_id)
    summary["progress_id"] = progress_dir.name
    manifest = _confirmation_progress_manifest(
        progress_dir=progress_dir,
        registry_id=registry_id,
        generated=generated,
        rows=rows,
        summary=summary,
    )
    report = render_confirmation_progress_report(manifest, rows, summary)
    progress_dir.mkdir(parents=True, exist_ok=False)
    _write_json(progress_dir / "confirmation_progress_manifest.json", manifest)
    _write_jsonl(progress_dir / "target_progress.jsonl", rows)
    _write_json(progress_dir / "target_progress_summary.json", summary)
    _write_json(progress_dir / "confirmation_progress_input_snapshot.json", snapshot)
    _write_text(progress_dir / "confirmation_progress_report.md", report)
    _update_latest_pointer(
        "latest_confirmation_progress",
        progress_dir.name,
        progress_dir / "confirmation_progress_manifest.json",
    )
    return {
        "progress_id": progress_dir.name,
        "progress_dir": progress_dir,
        "manifest": manifest,
        "target_progress": rows,
        "target_progress_summary": summary,
        "input_snapshot": snapshot,
    }


def confirmation_progress_report_payload(
    *,
    progress_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CONFIRMATION_PROGRESS_DIR,
) -> dict[str, Any]:
    progress_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=progress_id if not latest else None,
        pointer_name="latest_confirmation_progress",
    )
    return {
        **_read_json(progress_dir / "confirmation_progress_manifest.json"),
        "target_progress": _read_jsonl(progress_dir / "target_progress.jsonl"),
        "target_progress_summary": _read_json(progress_dir / "target_progress_summary.json"),
        "confirmation_progress_report": _read_text(
            progress_dir / "confirmation_progress_report.md"
        ),
        "progress_dir": str(progress_dir),
    }


def validate_confirmation_progress_artifact(
    *,
    progress_id: str,
    output_dir: Path = DEFAULT_CONFIRMATION_PROGRESS_DIR,
) -> dict[str, Any]:
    progress_dir = output_dir / progress_id
    manifest = _read_optional_json(progress_dir / "confirmation_progress_manifest.json") or {}
    rows = _read_jsonl(progress_dir / "target_progress.jsonl")
    summary = _read_optional_json(progress_dir / "target_progress_summary.json") or {}
    snapshot = _read_optional_json(progress_dir / "confirmation_progress_input_snapshot.json") or {}
    source_errors: list[str] = []
    recompute_error = ""
    expected_rows: list[dict[str, Any]] = []
    expected_summary: dict[str, Any] = {}
    expected_manifest: dict[str, Any] = {}
    expected_snapshot: dict[str, Any] = {}
    expected_report = ""
    try:
        if snapshot.get("schema_version") != CONFIRMATION_PROGRESS_SNAPSHOT_SCHEMA_VERSION:
            source_errors.append("progress snapshot schema mismatch")
        generated = _datetime_from_any(snapshot.get("generated_at"))
        registry_id = _text(snapshot.get("registry_id"))
        registry_root = Path(_text(snapshot.get("registry_root")))
        if generated is None or not registry_id:
            raise DynamicV3ConfirmationCycleError("progress snapshot identity/time invalid")
        live_registry_validation = validate_confirmation_targets_artifact(
            registry_id=registry_id, output_dir=registry_root
        )
        if live_registry_validation.get(
            "status"
        ) != "PASS" or live_registry_validation != snapshot.get("registry_validation"):
            source_errors.append("confirmation registry validation changed")
        live_registry_bundle = _confirmation_progress_registry_bundle(registry_root / registry_id)
        if live_registry_bundle != snapshot.get("registry_bundle"):
            source_errors.append("confirmation registry bundle changed")
        frozen_sources = _mapping(snapshot.get("evidence_sources"))
        expected_sources: dict[str, Any] = {}
        for kind, source_value in frozen_sources.items():
            source = _mapping(source_value)
            source_root = Path(_text(source.get("source_root")))
            expected_source = _select_confirmation_progress_source(
                kind=_text(kind), output_dir=source_root, generated=generated
            )
            expected_sources[_text(kind)] = expected_source
            if expected_source != source:
                source_errors.append(f"{kind} evidence selection changed")
        expected_inventory = [
            {
                "source_kind": kind,
                "selection_status": _mapping(source).get("selection_status"),
                "artifact_id": _mapping(source).get("artifact_id"),
                "source_dir": _mapping(source).get("source_dir"),
            }
            for kind, source in sorted(expected_sources.items())
        ]
        expected_snapshot = {
            "schema_version": CONFIRMATION_PROGRESS_SNAPSHOT_SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_confirmation_progress_input_snapshot",
            "generated_at": generated.isoformat(),
            "registry_id": registry_id,
            "registry_root": str(registry_root),
            "registry_bundle": live_registry_bundle,
            "registry_validation": live_registry_validation,
            "evidence_sources": expected_sources,
            "selection_inventory": expected_inventory,
            "production_effect": "none",
        }
        expected_rows = _confirmation_progress_rows_from_snapshot(expected_snapshot)
        expected_summary = _progress_summary(expected_rows, registry_id, generated)
        expected_summary["progress_id"] = progress_id
        expected_manifest = _confirmation_progress_manifest(
            progress_dir=progress_dir,
            registry_id=registry_id,
            generated=generated,
            rows=expected_rows,
            summary=expected_summary,
        )
        expected_report = render_confirmation_progress_report(
            expected_manifest, expected_rows, expected_summary
        )
    except Exception as exc:  # noqa: BLE001
        recompute_error = str(exc)
    checks = [
        _check(
            "manifest_exists",
            (progress_dir / "confirmation_progress_manifest.json").exists(),
            "",
        ),
        _check("progress_jsonl_exists", (progress_dir / "target_progress.jsonl").exists(), ""),
        _check("summary_exists", (progress_dir / "target_progress_summary.json").exists(), ""),
        _check("report_exists", (progress_dir / "confirmation_progress_report.md").exists(), ""),
        _check(
            "snapshot_exists",
            (progress_dir / "confirmation_progress_input_snapshot.json").exists(),
            "",
        ),
        _check("progress_id_matches", manifest.get("progress_id") == progress_id, ""),
        _check(
            "progress_status_valid",
            all(row.get("progress_status") in PROGRESS_STATUSES for row in rows),
            "progress_status",
        ),
        _check(
            "not_ready_without_required_events",
            all(
                row.get("progress_status") != "READY_FOR_EVALUATION"
                or _available_event_count(row) >= _required_event_count(row)
                for row in rows
            ),
            "required event floor",
        ),
        _check("source_snapshot_valid", not source_errors, ",".join(source_errors)),
        _check("views_recomputed", not recompute_error, recompute_error),
        _check("snapshot_recomputed", snapshot == expected_snapshot, "live sources"),
        _check("rows_recomputed", rows == expected_rows, "snapshot"),
        _check("summary_recomputed", summary == expected_summary, "snapshot"),
        _check("manifest_recomputed", manifest == expected_manifest, "snapshot"),
        _check(
            "json_bytes_recomputed",
            all(
                path.is_file()
                and _read_text(path)
                == json.dumps(_jsonable(payload), ensure_ascii=False, indent=2, sort_keys=True)
                + "\n"
                for path, payload in (
                    (progress_dir / "confirmation_progress_manifest.json", expected_manifest),
                    (progress_dir / "target_progress_summary.json", expected_summary),
                    (progress_dir / "confirmation_progress_input_snapshot.json", expected_snapshot),
                )
            ),
            "canonical JSON bytes",
        ),
        _check(
            "jsonl_bytes_recomputed",
            (progress_dir / "target_progress.jsonl").is_file()
            and _read_text(progress_dir / "target_progress.jsonl")
            == "".join(
                json.dumps(_jsonable(row), ensure_ascii=False, sort_keys=True) + "\n"
                for row in expected_rows
            ),
            "canonical JSONL bytes",
        ),
        _check(
            "report_recomputed",
            (progress_dir / "confirmation_progress_report.md").is_file()
            and _read_text(progress_dir / "confirmation_progress_report.md") == expected_report,
            "report bytes",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_confirmation_progress_validation",
        artifact_id_key="progress_id",
        artifact_id=progress_id,
        checks=checks,
    )


def _confirmation_evaluation_progress_bundle(progress_dir: Path) -> dict[str, Any]:
    json_files = (
        "confirmation_progress_manifest.json",
        "target_progress_summary.json",
        "confirmation_progress_input_snapshot.json",
    )
    jsonl_files = ("target_progress.jsonl",)
    text_files = ("confirmation_progress_report.md",)
    return _bounded_source_bundle(
        source_dir=progress_dir,
        canonical_files=json_files + jsonl_files + text_files,
        json_views=("confirmation_progress_manifest.json", "target_progress_summary.json"),
        jsonl_views=jsonl_files,
    )


def _confirmation_evaluation_rows_from_snapshot(
    snapshot: Mapping[str, Any],
) -> list[dict[str, Any]]:
    generated = _datetime_from_any(snapshot.get("generated_at"))
    if generated is None:
        raise DynamicV3ConfirmationCycleError("evaluation snapshot generated_at invalid")
    progress_bundle = _mapping(snapshot.get("progress_bundle"))
    progress_rows = _records(_mapping(progress_bundle.get("jsonl")).get("target_progress.jsonl"))
    target_ids = [_text(row.get("target_id")) for row in progress_rows]
    if (
        not progress_rows
        or any(not target_id for target_id in target_ids)
        or len(target_ids) != len(set(target_ids))
    ):
        raise DynamicV3ConfirmationCycleError("progress targets missing or duplicate")
    return [_evaluation_row_v2(row, generated=generated) for row in progress_rows]


def _confirmation_evaluation_manifest(
    *,
    evaluation_dir: Path,
    progress_id: str,
    registry_id: str,
    generated: datetime,
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_evaluation_manifest",
        "evaluation_id": evaluation_dir.name,
        "progress_id": progress_id,
        "registry_id": registry_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if rows else "FAIL",
        "rule_review_ready": summary["rule_review_ready"],
        "confirmation_evaluation_manifest_path": str(
            evaluation_dir / "confirmation_evaluation_manifest.json"
        ),
        "target_evaluations_path": str(evaluation_dir / "target_evaluations.jsonl"),
        "confirmation_evaluation_summary_path": str(
            evaluation_dir / "confirmation_evaluation_summary.json"
        ),
        "confirmation_evaluation_input_snapshot_path": str(
            evaluation_dir / "confirmation_evaluation_input_snapshot.json"
        ),
        "confirmation_evaluation_report_path": str(
            evaluation_dir / "confirmation_evaluation_report.md"
        ),
        "auto_apply": False,
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def run_confirmation_evaluation(
    *,
    progress_id: str,
    progress_dir: Path = DEFAULT_CONFIRMATION_PROGRESS_DIR,
    output_dir: Path = DEFAULT_CONFIRMATION_EVALUATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    if generated.tzinfo is None or generated.utcoffset() is None:
        raise DynamicV3ConfirmationCycleError("generated_at must be timezone-aware")
    generated = generated.astimezone(UTC)
    progress_validation = validate_confirmation_progress_artifact(
        progress_id=progress_id, output_dir=progress_dir
    )
    if progress_validation.get("status") != "PASS":
        raise DynamicV3ConfirmationCycleError("confirmation progress validation failed")
    progress_artifact_dir = progress_dir / progress_id
    progress_bundle = _confirmation_evaluation_progress_bundle(progress_artifact_dir)
    progress_manifest = _mapping(
        _mapping(progress_bundle.get("json")).get("confirmation_progress_manifest.json")
    )
    progress_generated = _datetime_from_any(progress_manifest.get("generated_at"))
    if progress_generated is None or progress_generated > generated:
        raise DynamicV3ConfirmationCycleError("confirmation progress generated after cutoff")
    snapshot = {
        "schema_version": CONFIRMATION_EVALUATION_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_evaluation_input_snapshot",
        "generated_at": generated.isoformat(),
        "progress_id": progress_id,
        "progress_root": str(progress_dir),
        "progress_bundle": progress_bundle,
        "progress_validation": progress_validation,
        "lineage": {
            "progress_id": progress_id,
            "registry_id": progress_manifest.get("registry_id"),
        },
        "production_effect": "none",
    }
    rows = _confirmation_evaluation_rows_from_snapshot(snapshot)
    summary = _evaluation_summary(rows, progress_id, generated)
    evaluation_id = _stable_id("confirmation-evaluation", progress_id, generated)
    evaluation_dir = _unique_dir(output_dir / evaluation_id)
    summary["evaluation_id"] = evaluation_dir.name
    manifest = _confirmation_evaluation_manifest(
        evaluation_dir=evaluation_dir,
        progress_id=progress_id,
        registry_id=_text(progress_manifest.get("registry_id")),
        generated=generated,
        rows=rows,
        summary=summary,
    )
    report = render_confirmation_evaluation_report(manifest, rows, summary)
    evaluation_dir.mkdir(parents=True, exist_ok=False)
    _write_json(evaluation_dir / "confirmation_evaluation_manifest.json", manifest)
    _write_jsonl(evaluation_dir / "target_evaluations.jsonl", rows)
    _write_json(evaluation_dir / "confirmation_evaluation_summary.json", summary)
    _write_json(evaluation_dir / "confirmation_evaluation_input_snapshot.json", snapshot)
    _write_text(evaluation_dir / "confirmation_evaluation_report.md", report)
    _update_latest_pointer(
        "latest_confirmation_evaluation",
        evaluation_dir.name,
        evaluation_dir / "confirmation_evaluation_manifest.json",
    )
    return {
        "evaluation_id": evaluation_dir.name,
        "evaluation_dir": evaluation_dir,
        "manifest": manifest,
        "target_evaluations": rows,
        "confirmation_evaluation_summary": summary,
        "input_snapshot": snapshot,
    }


def confirmation_evaluation_report_payload(
    *,
    evaluation_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CONFIRMATION_EVALUATION_DIR,
) -> dict[str, Any]:
    evaluation_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=evaluation_id if not latest else None,
        pointer_name="latest_confirmation_evaluation",
    )
    return {
        **_read_json(evaluation_dir / "confirmation_evaluation_manifest.json"),
        "target_evaluations": _read_jsonl(evaluation_dir / "target_evaluations.jsonl"),
        "confirmation_evaluation_summary": _read_json(
            evaluation_dir / "confirmation_evaluation_summary.json"
        ),
        "confirmation_evaluation_report": _read_text(
            evaluation_dir / "confirmation_evaluation_report.md"
        ),
        "evaluation_dir": str(evaluation_dir),
    }


def validate_confirmation_evaluation_artifact(
    *,
    evaluation_id: str,
    output_dir: Path = DEFAULT_CONFIRMATION_EVALUATION_DIR,
) -> dict[str, Any]:
    evaluation_dir = output_dir / evaluation_id
    manifest = _read_optional_json(evaluation_dir / "confirmation_evaluation_manifest.json") or {}
    rows = _read_jsonl(evaluation_dir / "target_evaluations.jsonl")
    summary = _read_optional_json(evaluation_dir / "confirmation_evaluation_summary.json") or {}
    snapshot = (
        _read_optional_json(evaluation_dir / "confirmation_evaluation_input_snapshot.json") or {}
    )
    source_errors: list[str] = []
    recompute_error = ""
    expected_rows: list[dict[str, Any]] = []
    expected_summary: dict[str, Any] = {}
    expected_manifest: dict[str, Any] = {}
    expected_snapshot: dict[str, Any] = {}
    expected_report = ""
    try:
        if snapshot.get("schema_version") != CONFIRMATION_EVALUATION_SNAPSHOT_SCHEMA_VERSION:
            source_errors.append("evaluation snapshot schema mismatch")
        generated = _datetime_from_any(snapshot.get("generated_at"))
        progress_id = _text(snapshot.get("progress_id"))
        progress_root = Path(_text(snapshot.get("progress_root")))
        if generated is None or not progress_id:
            raise DynamicV3ConfirmationCycleError("evaluation snapshot identity/time invalid")
        live_validation = validate_confirmation_progress_artifact(
            progress_id=progress_id, output_dir=progress_root
        )
        if live_validation.get("status") != "PASS":
            source_errors.append("confirmation progress validation failed")
        live_bundle = _confirmation_evaluation_progress_bundle(progress_root / progress_id)
        progress_manifest = _mapping(
            _mapping(live_bundle.get("json")).get("confirmation_progress_manifest.json")
        )
        progress_generated = _datetime_from_any(progress_manifest.get("generated_at"))
        if progress_generated is None or progress_generated > generated:
            source_errors.append("confirmation progress after cutoff")
        expected_snapshot = {
            "schema_version": CONFIRMATION_EVALUATION_SNAPSHOT_SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_confirmation_evaluation_input_snapshot",
            "generated_at": generated.isoformat(),
            "progress_id": progress_id,
            "progress_root": str(progress_root),
            "progress_bundle": live_bundle,
            "progress_validation": live_validation,
            "lineage": {
                "progress_id": progress_id,
                "registry_id": progress_manifest.get("registry_id"),
            },
            "production_effect": "none",
        }
        if live_bundle != snapshot.get("progress_bundle"):
            source_errors.append("confirmation progress bundle changed")
        if live_validation != snapshot.get("progress_validation"):
            source_errors.append("confirmation progress validation changed")
        expected_rows = _confirmation_evaluation_rows_from_snapshot(expected_snapshot)
        expected_summary = _evaluation_summary(expected_rows, progress_id, generated)
        expected_summary["evaluation_id"] = evaluation_id
        expected_manifest = _confirmation_evaluation_manifest(
            evaluation_dir=evaluation_dir,
            progress_id=progress_id,
            registry_id=_text(progress_manifest.get("registry_id")),
            generated=generated,
            rows=expected_rows,
            summary=expected_summary,
        )
        expected_report = render_confirmation_evaluation_report(
            expected_manifest, expected_rows, expected_summary
        )
    except Exception as exc:  # noqa: BLE001
        recompute_error = str(exc)
    checks = [
        _check(
            "manifest_exists",
            (evaluation_dir / "confirmation_evaluation_manifest.json").exists(),
            "",
        ),
        _check("evaluations_exists", (evaluation_dir / "target_evaluations.jsonl").exists(), ""),
        _check(
            "summary_exists",
            (evaluation_dir / "confirmation_evaluation_summary.json").exists(),
            "",
        ),
        _check(
            "report_exists",
            (evaluation_dir / "confirmation_evaluation_report.md").exists(),
            "",
        ),
        _check(
            "snapshot_exists",
            (evaluation_dir / "confirmation_evaluation_input_snapshot.json").exists(),
            "",
        ),
        _check("evaluation_id_matches", manifest.get("evaluation_id") == evaluation_id, ""),
        _check(
            "evaluation_status_valid",
            all(row.get("evaluation_status") in EVALUATION_STATUSES for row in rows),
            "evaluation_status",
        ),
        _check(
            "not_ready_respects_progress",
            all(
                row.get("evaluation_status") == "NOT_READY"
                and not _records(row.get("failure_conditions_triggered"))
                and all(
                    result.get("status") == "NOT_EVALUATED" and result.get("actual") is None
                    for result in _mapping(row.get("criteria_results")).values()
                    if isinstance(result, Mapping)
                )
                for row in rows
                if row.get("progress_status") != "READY_FOR_EVALUATION"
            ),
            "not ready",
        ),
        _check("source_snapshot_valid", not source_errors, ",".join(source_errors)),
        _check("views_recomputed", not recompute_error, recompute_error),
        _check("snapshot_recomputed", snapshot == expected_snapshot, "live progress"),
        _check("rows_recomputed", rows == expected_rows, "snapshot"),
        _check("summary_recomputed", summary == expected_summary, "snapshot"),
        _check("manifest_recomputed", manifest == expected_manifest, "snapshot"),
        _check(
            "json_bytes_recomputed",
            all(
                path.is_file()
                and _read_text(path)
                == json.dumps(_jsonable(payload), ensure_ascii=False, indent=2, sort_keys=True)
                + "\n"
                for path, payload in (
                    (evaluation_dir / "confirmation_evaluation_manifest.json", expected_manifest),
                    (evaluation_dir / "confirmation_evaluation_summary.json", expected_summary),
                    (
                        evaluation_dir / "confirmation_evaluation_input_snapshot.json",
                        expected_snapshot,
                    ),
                )
            ),
            "canonical JSON bytes",
        ),
        _check(
            "jsonl_bytes_recomputed",
            (evaluation_dir / "target_evaluations.jsonl").is_file()
            and _read_text(evaluation_dir / "target_evaluations.jsonl")
            == "".join(
                json.dumps(_jsonable(row), ensure_ascii=False, sort_keys=True) + "\n"
                for row in expected_rows
            ),
            "canonical JSONL bytes",
        ),
        _check(
            "report_recomputed",
            (evaluation_dir / "confirmation_evaluation_report.md").is_file()
            and _read_text(evaluation_dir / "confirmation_evaluation_report.md") == expected_report,
            "report bytes",
        ),
        _check("auto_apply_forbidden", manifest.get("auto_apply") is False, "auto apply"),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_confirmation_evaluation_validation",
        artifact_id_key="evaluation_id",
        artifact_id=evaluation_id,
        checks=checks,
    )


def _rule_review_cycle_evaluation_bundle(evaluation_dir: Path) -> dict[str, Any]:
    json_files = (
        "confirmation_evaluation_manifest.json",
        "confirmation_evaluation_summary.json",
        "confirmation_evaluation_input_snapshot.json",
    )
    jsonl_files = ("target_evaluations.jsonl",)
    text_files = ("confirmation_evaluation_report.md",)
    return _bounded_source_bundle(
        source_dir=evaluation_dir,
        canonical_files=json_files + jsonl_files + text_files,
        json_views=(
            "confirmation_evaluation_manifest.json",
            "confirmation_evaluation_summary.json",
        ),
        jsonl_views=jsonl_files,
    )


def _bounded_source_bundle(
    *,
    source_dir: Path,
    canonical_files: Sequence[str],
    json_views: Sequence[str] = (),
    jsonl_views: Sequence[str] = (),
    yaml_views: Sequence[str] = (),
) -> dict[str, Any]:
    names = list(canonical_files)
    if not names or len(names) != len(set(names)):
        raise DynamicV3ConfirmationCycleError("source bundle files missing or duplicate")
    files: dict[str, Any] = {}
    for name in names:
        path = source_dir / name
        if not path.is_file():
            raise DynamicV3ConfirmationCycleError(f"source bundle file missing: {path}")
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        files[name] = {
            "path": str(path),
            "size_bytes": path.stat().st_size,
            "sha256": digest.hexdigest(),
        }
    return {
        "schema_version": "content_commitment_bundle.v1",
        "source_dir": str(source_dir),
        "canonical_file_count": len(names),
        "files": files,
        "json": {name: _read_json(source_dir / name) for name in json_views},
        "jsonl": {name: _read_jsonl(source_dir / name) for name in jsonl_views},
        "yaml": {name: _read_yaml(source_dir / name) for name in yaml_views},
    }


def _strict_rule_review_target_ids(
    rows: Sequence[Mapping[str, Any]], *, source_name: str
) -> list[str]:
    target_ids = [_text(row.get("target_id")) for row in rows]
    if (
        not rows
        or any(not target_id for target_id in target_ids)
        or len(target_ids) != len(set(target_ids))
    ):
        raise DynamicV3ConfirmationCycleError(
            f"rule review {source_name} targets missing or duplicate"
        )
    return target_ids


def _rule_review_cycle_source_views(
    snapshot: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    generated = _datetime_from_any(snapshot.get("generated_at"))
    registry_id = _text(snapshot.get("registry_id"))
    progress_id = _text(snapshot.get("progress_id"))
    evaluation_id = _text(snapshot.get("evaluation_id"))
    if generated is None or not registry_id or not progress_id or not evaluation_id:
        raise DynamicV3ConfirmationCycleError("rule review snapshot identity/time invalid")
    registry_bundle = _mapping(snapshot.get("registry_bundle"))
    progress_bundle = _mapping(snapshot.get("progress_bundle"))
    evaluation_bundle = _mapping(snapshot.get("evaluation_bundle"))
    registry_manifest = _mapping(
        _mapping(registry_bundle.get("json")).get("confirmation_registry_manifest.json")
    )
    progress_manifest = _mapping(
        _mapping(progress_bundle.get("json")).get("confirmation_progress_manifest.json")
    )
    evaluation_manifest = _mapping(
        _mapping(evaluation_bundle.get("json")).get("confirmation_evaluation_manifest.json")
    )
    registry_generated = _datetime_from_any(registry_manifest.get("generated_at"))
    progress_generated = _datetime_from_any(progress_manifest.get("generated_at"))
    evaluation_generated = _datetime_from_any(evaluation_manifest.get("generated_at"))
    if (
        registry_manifest.get("registry_id") != registry_id
        or progress_manifest.get("progress_id") != progress_id
        or progress_manifest.get("registry_id") != registry_id
        or evaluation_manifest.get("evaluation_id") != evaluation_id
        or evaluation_manifest.get("progress_id") != progress_id
        or evaluation_manifest.get("registry_id") != registry_id
    ):
        raise DynamicV3ConfirmationCycleError("rule review source lineage mismatch")
    if (
        registry_generated is None
        or progress_generated is None
        or evaluation_generated is None
        or not registry_generated <= progress_generated <= evaluation_generated <= generated
    ):
        raise DynamicV3ConfirmationCycleError("rule review source chronology invalid")
    registry_targets = _records(
        _mapping(_mapping(registry_bundle.get("yaml")).get("registered_targets.yaml")).get(
            "targets"
        )
    )
    progress_rows = _records(_mapping(progress_bundle.get("jsonl")).get("target_progress.jsonl"))
    evaluation_rows = _records(
        _mapping(evaluation_bundle.get("jsonl")).get("target_evaluations.jsonl")
    )
    registry_target_ids = _strict_rule_review_target_ids(registry_targets, source_name="registry")
    progress_target_ids = _strict_rule_review_target_ids(progress_rows, source_name="progress")
    evaluation_target_ids = _strict_rule_review_target_ids(
        evaluation_rows, source_name="evaluation"
    )
    if set(registry_target_ids) != set(progress_target_ids) or set(registry_target_ids) != set(
        evaluation_target_ids
    ):
        raise DynamicV3ConfirmationCycleError("rule review source target coverage mismatch")
    progress_by_target = {_text(row.get("target_id")): dict(row) for row in progress_rows}
    evaluation_by_target = {_text(row.get("target_id")): dict(row) for row in evaluation_rows}
    for target in registry_targets:
        target_id = _text(target.get("target_id"))
        progress = progress_by_target[target_id]
        evaluation = evaluation_by_target[target_id]
        if progress.get("target_status") != target.get("status") or evaluation.get(
            "progress_status"
        ) != progress.get("progress_status"):
            raise DynamicV3ConfirmationCycleError(
                f"rule review target state lineage mismatch: {target_id}"
            )
    return registry_targets, progress_by_target, evaluation_by_target


def _rule_review_cycle_decision_matrix(
    *,
    cycle_id: str,
    registry_id: str,
    progress_id: str,
    evaluation_id: str,
    generated: datetime,
    registry_targets: Sequence[Mapping[str, Any]],
    progress_rows: Mapping[str, Mapping[str, Any]],
    evaluation_rows: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    decision_targets = [
        _rule_review_decision_row(
            target,
            progress=progress_rows[_text(target.get("target_id"))],
            evaluation=evaluation_rows[_text(target.get("target_id"))],
        )
        for target in registry_targets
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_rule_review_decision_matrix",
        "cycle_id": cycle_id,
        "registry_id": registry_id,
        "progress_id": progress_id,
        "evaluation_id": evaluation_id,
        "generated_at": generated.isoformat(),
        "targets": decision_targets,
        "cycle_recommendation": _cycle_recommendation(decision_targets),
        "policy_change_allowed": False,
        "auto_apply": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _rule_review_cycle_manifest(
    *,
    cycle_dir: Path,
    generated: datetime,
    decision_matrix: Mapping[str, Any],
) -> dict[str, Any]:
    targets = _records(decision_matrix.get("targets"))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_rule_review_cycle_manifest",
        "cycle_id": cycle_dir.name,
        "registry_id": decision_matrix.get("registry_id"),
        "progress_id": decision_matrix.get("progress_id"),
        "evaluation_id": decision_matrix.get("evaluation_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if targets else "FAIL",
        "cycle_recommendation": decision_matrix.get("cycle_recommendation"),
        "targets_requiring_owner_action": sum(
            1 for row in targets if row.get("owner_action_required") is True
        ),
        "policy_change_allowed": False,
        "rule_review_cycle_manifest_path": str(cycle_dir / "rule_review_cycle_manifest.json"),
        "rule_review_decision_matrix_path": str(cycle_dir / "rule_review_decision_matrix.json"),
        "rule_review_cycle_input_snapshot_path": str(
            cycle_dir / "rule_review_cycle_input_snapshot.json"
        ),
        "rule_review_cycle_report_path": str(cycle_dir / "rule_review_cycle_report.md"),
        "reader_brief_section_path": str(cycle_dir / "reader_brief_section.md"),
        "auto_apply": False,
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def run_rule_review_cycle(
    *,
    registry_id: str,
    progress_id: str,
    evaluation_id: str,
    registry_dir: Path = DEFAULT_CONFIRMATION_REGISTRY_DIR,
    progress_dir: Path = DEFAULT_CONFIRMATION_PROGRESS_DIR,
    evaluation_dir: Path = DEFAULT_CONFIRMATION_EVALUATION_DIR,
    output_dir: Path = DEFAULT_RULE_REVIEW_CYCLE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    if generated.tzinfo is None or generated.utcoffset() is None:
        raise DynamicV3ConfirmationCycleError("generated_at must be timezone-aware")
    generated = generated.astimezone(UTC)
    registry_validation = validate_confirmation_targets_artifact(
        registry_id=registry_id, output_dir=registry_dir
    )
    progress_validation = validate_confirmation_progress_artifact(
        progress_id=progress_id, output_dir=progress_dir
    )
    evaluation_validation = validate_confirmation_evaluation_artifact(
        evaluation_id=evaluation_id, output_dir=evaluation_dir
    )
    if registry_validation.get("status") != "PASS":
        raise DynamicV3ConfirmationCycleError("confirmation registry validation failed")
    if progress_validation.get("status") != "PASS":
        raise DynamicV3ConfirmationCycleError("confirmation progress validation failed")
    if evaluation_validation.get("status") != "PASS":
        raise DynamicV3ConfirmationCycleError("confirmation evaluation validation failed")
    snapshot = {
        "schema_version": RULE_REVIEW_CYCLE_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_rule_review_cycle_input_snapshot",
        "generated_at": generated.isoformat(),
        "registry_id": registry_id,
        "registry_root": str(registry_dir),
        "registry_bundle": _confirmation_progress_registry_bundle(registry_dir / registry_id),
        "registry_validation": registry_validation,
        "progress_id": progress_id,
        "progress_root": str(progress_dir),
        "progress_bundle": _confirmation_evaluation_progress_bundle(progress_dir / progress_id),
        "progress_validation": progress_validation,
        "evaluation_id": evaluation_id,
        "evaluation_root": str(evaluation_dir),
        "evaluation_bundle": _rule_review_cycle_evaluation_bundle(evaluation_dir / evaluation_id),
        "evaluation_validation": evaluation_validation,
        "lineage": {
            "registry_id": registry_id,
            "progress_id": progress_id,
            "evaluation_id": evaluation_id,
        },
        "production_effect": "none",
    }
    registry_targets, progress_rows, evaluation_rows = _rule_review_cycle_source_views(snapshot)
    cycle_id = _stable_id("rule-review-cycle", registry_id, progress_id, evaluation_id, generated)
    cycle_dir = _unique_dir(output_dir / cycle_id)
    decision_matrix = _rule_review_cycle_decision_matrix(
        cycle_id=cycle_dir.name,
        registry_id=registry_id,
        progress_id=progress_id,
        evaluation_id=evaluation_id,
        generated=generated,
        registry_targets=registry_targets,
        progress_rows=progress_rows,
        evaluation_rows=evaluation_rows,
    )
    manifest = _rule_review_cycle_manifest(
        cycle_dir=cycle_dir,
        generated=generated,
        decision_matrix=decision_matrix,
    )
    report = render_rule_review_cycle_report(manifest, decision_matrix)
    reader_brief = render_rule_review_reader_brief(decision_matrix)
    cycle_dir.mkdir(parents=True, exist_ok=False)
    _write_json(cycle_dir / "rule_review_cycle_manifest.json", manifest)
    _write_json(cycle_dir / "rule_review_decision_matrix.json", decision_matrix)
    _write_json(cycle_dir / "rule_review_cycle_input_snapshot.json", snapshot)
    _write_text(cycle_dir / "rule_review_cycle_report.md", report)
    _write_text(cycle_dir / "reader_brief_section.md", reader_brief)
    _update_latest_pointer(
        "latest_rule_review_cycle",
        cycle_dir.name,
        cycle_dir / "rule_review_cycle_manifest.json",
    )
    return {
        "cycle_id": cycle_dir.name,
        "cycle_dir": cycle_dir,
        "manifest": manifest,
        "rule_review_decision_matrix": decision_matrix,
        "input_snapshot": snapshot,
        "reader_brief_section": reader_brief,
    }


def rule_review_cycle_report_payload(
    *,
    cycle_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_RULE_REVIEW_CYCLE_DIR,
) -> dict[str, Any]:
    cycle_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=cycle_id if not latest else None,
        pointer_name="latest_rule_review_cycle",
    )
    return {
        **_read_json(cycle_dir / "rule_review_cycle_manifest.json"),
        "rule_review_decision_matrix": _read_json(cycle_dir / "rule_review_decision_matrix.json"),
        "input_snapshot": _read_json(cycle_dir / "rule_review_cycle_input_snapshot.json"),
        "reader_brief_section": _read_text(cycle_dir / "reader_brief_section.md"),
        "rule_review_cycle_report": _read_text(cycle_dir / "rule_review_cycle_report.md"),
        "cycle_dir": str(cycle_dir),
    }


def validate_rule_review_cycle_artifact(
    *,
    cycle_id: str,
    output_dir: Path = DEFAULT_RULE_REVIEW_CYCLE_DIR,
) -> dict[str, Any]:
    cycle_dir = output_dir / cycle_id
    manifest = _read_optional_json(cycle_dir / "rule_review_cycle_manifest.json") or {}
    matrix = _read_optional_json(cycle_dir / "rule_review_decision_matrix.json") or {}
    snapshot = _read_optional_json(cycle_dir / "rule_review_cycle_input_snapshot.json") or {}
    targets = _records(matrix.get("targets"))
    source_errors: list[str] = []
    recompute_error = ""
    expected_snapshot: dict[str, Any] = {}
    expected_matrix: dict[str, Any] = {}
    expected_manifest: dict[str, Any] = {}
    expected_report = ""
    expected_reader_brief = ""
    try:
        if snapshot.get("schema_version") != RULE_REVIEW_CYCLE_SNAPSHOT_SCHEMA_VERSION:
            source_errors.append("rule review snapshot schema mismatch")
        generated = _datetime_from_any(snapshot.get("generated_at"))
        registry_id = _text(snapshot.get("registry_id"))
        progress_id = _text(snapshot.get("progress_id"))
        evaluation_id = _text(snapshot.get("evaluation_id"))
        registry_root = Path(_text(snapshot.get("registry_root")))
        progress_root = Path(_text(snapshot.get("progress_root")))
        evaluation_root = Path(_text(snapshot.get("evaluation_root")))
        if generated is None or not registry_id or not progress_id or not evaluation_id:
            raise DynamicV3ConfirmationCycleError("rule review snapshot identity/time invalid")
        registry_validation = validate_confirmation_targets_artifact(
            registry_id=registry_id, output_dir=registry_root
        )
        progress_validation = validate_confirmation_progress_artifact(
            progress_id=progress_id, output_dir=progress_root
        )
        evaluation_validation = validate_confirmation_evaluation_artifact(
            evaluation_id=evaluation_id, output_dir=evaluation_root
        )
        for source_name, validation in (
            ("registry", registry_validation),
            ("progress", progress_validation),
            ("evaluation", evaluation_validation),
        ):
            if validation.get("status") != "PASS":
                source_errors.append(f"confirmation {source_name} validation failed")
        registry_bundle = _confirmation_progress_registry_bundle(registry_root / registry_id)
        progress_bundle = _confirmation_evaluation_progress_bundle(progress_root / progress_id)
        evaluation_bundle = _rule_review_cycle_evaluation_bundle(evaluation_root / evaluation_id)
        expected_snapshot = {
            "schema_version": RULE_REVIEW_CYCLE_SNAPSHOT_SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_rule_review_cycle_input_snapshot",
            "generated_at": generated.isoformat(),
            "registry_id": registry_id,
            "registry_root": str(registry_root),
            "registry_bundle": registry_bundle,
            "registry_validation": registry_validation,
            "progress_id": progress_id,
            "progress_root": str(progress_root),
            "progress_bundle": progress_bundle,
            "progress_validation": progress_validation,
            "evaluation_id": evaluation_id,
            "evaluation_root": str(evaluation_root),
            "evaluation_bundle": evaluation_bundle,
            "evaluation_validation": evaluation_validation,
            "lineage": {
                "registry_id": registry_id,
                "progress_id": progress_id,
                "evaluation_id": evaluation_id,
            },
            "production_effect": "none",
        }
        for source_name, live, frozen in (
            ("registry bundle", registry_bundle, snapshot.get("registry_bundle")),
            ("registry validation", registry_validation, snapshot.get("registry_validation")),
            ("progress bundle", progress_bundle, snapshot.get("progress_bundle")),
            ("progress validation", progress_validation, snapshot.get("progress_validation")),
            ("evaluation bundle", evaluation_bundle, snapshot.get("evaluation_bundle")),
            (
                "evaluation validation",
                evaluation_validation,
                snapshot.get("evaluation_validation"),
            ),
        ):
            if live != frozen:
                source_errors.append(f"{source_name} changed")
        registry_targets, progress_rows, evaluation_rows = _rule_review_cycle_source_views(
            expected_snapshot
        )
        expected_matrix = _rule_review_cycle_decision_matrix(
            cycle_id=cycle_id,
            registry_id=registry_id,
            progress_id=progress_id,
            evaluation_id=evaluation_id,
            generated=generated,
            registry_targets=registry_targets,
            progress_rows=progress_rows,
            evaluation_rows=evaluation_rows,
        )
        expected_manifest = _rule_review_cycle_manifest(
            cycle_dir=cycle_dir,
            generated=generated,
            decision_matrix=expected_matrix,
        )
        expected_report = render_rule_review_cycle_report(expected_manifest, expected_matrix)
        expected_reader_brief = render_rule_review_reader_brief(expected_matrix)
    except Exception as exc:  # noqa: BLE001
        recompute_error = str(exc)
    checks = [
        _check("manifest_exists", (cycle_dir / "rule_review_cycle_manifest.json").exists(), ""),
        _check("matrix_exists", (cycle_dir / "rule_review_decision_matrix.json").exists(), ""),
        _check(
            "snapshot_exists",
            (cycle_dir / "rule_review_cycle_input_snapshot.json").exists(),
            "",
        ),
        _check("report_exists", (cycle_dir / "rule_review_cycle_report.md").exists(), ""),
        _check("reader_brief_exists", (cycle_dir / "reader_brief_section.md").exists(), ""),
        _check("cycle_id_matches", manifest.get("cycle_id") == cycle_id, ""),
        _check(
            "decision_values_valid",
            all(row.get("rule_review_decision") in RULE_REVIEW_DECISIONS for row in targets),
            "rule_review_decision",
        ),
        _check("source_snapshot_valid", not source_errors, ",".join(source_errors)),
        _check("views_recomputed", not recompute_error, recompute_error),
        _check("snapshot_recomputed", snapshot == expected_snapshot, "live sources"),
        _check("matrix_recomputed", matrix == expected_matrix, "snapshot"),
        _check("manifest_recomputed", manifest == expected_manifest, "snapshot"),
        _check(
            "json_bytes_recomputed",
            all(
                path.is_file()
                and _read_text(path)
                == json.dumps(_jsonable(payload), ensure_ascii=False, indent=2, sort_keys=True)
                + "\n"
                for path, payload in (
                    (cycle_dir / "rule_review_cycle_manifest.json", expected_manifest),
                    (cycle_dir / "rule_review_decision_matrix.json", expected_matrix),
                    (cycle_dir / "rule_review_cycle_input_snapshot.json", expected_snapshot),
                )
            ),
            "canonical JSON bytes",
        ),
        _check(
            "report_recomputed",
            (cycle_dir / "rule_review_cycle_report.md").is_file()
            and _read_text(cycle_dir / "rule_review_cycle_report.md") == expected_report,
            "report bytes",
        ),
        _check(
            "reader_brief_recomputed",
            (cycle_dir / "reader_brief_section.md").is_file()
            and _read_text(cycle_dir / "reader_brief_section.md") == expected_reader_brief,
            "reader brief bytes",
        ),
        _check(
            "policy_change_disallowed",
            manifest.get("policy_change_allowed") is False
            and matrix.get("policy_change_allowed") is False
            and all(row.get("policy_change_allowed") is False for row in targets),
            "policy_change_allowed=false",
        ),
        _check("auto_apply_forbidden", manifest.get("auto_apply") is False, "auto apply"),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_rule_review_cycle_validation",
        artifact_id_key="cycle_id",
        artifact_id=cycle_id,
        checks=checks,
    )


def create_rule_owner_decision(
    *,
    cycle_id: str,
    cycle_dir: Path = DEFAULT_RULE_REVIEW_CYCLE_DIR,
    journal_path: Path = DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _rule_owner_decision_generated_at(generated_at)
    source_snapshot = _rule_owner_decision_source_snapshot(
        cycle_id=cycle_id,
        cycle_root=cycle_dir,
        captured_at=generated,
    )
    events = _rule_owner_decision_events_for_write(journal_path)
    existing_records = _materialize_rule_owner_decisions(events)
    if any(record.get("cycle_id") == cycle_id for record in existing_records):
        raise DynamicV3ConfirmationCycleError(
            f"owner decision already exists for cycle: {cycle_id}"
        )
    decision_id = _stable_id("rule-owner-decision", cycle_id, generated)
    decision_dir = journal_path.parent / decision_id
    if decision_dir.exists():
        raise DynamicV3ConfirmationCycleError(
            f"owner decision artifact already exists: {decision_dir}"
        )
    snapshot_path = decision_dir / "rule_owner_decision_source_snapshot.json"
    snapshot_sha256 = _rule_owner_decision_payload_sha256(source_snapshot)
    event = _rule_owner_decision_event(
        event_type="DECISION_CREATED",
        decision_id=decision_id,
        cycle_id=cycle_id,
        event_at=generated.isoformat(),
        target_ids=source_snapshot["target_ids"],
        decision_scope_reason=source_snapshot["decision_scope_reason"],
        recommended_cycle_action=source_snapshot["recommended_cycle_action"],
        allowed_owner_decisions=source_snapshot["allowed_owner_decisions"],
        owner_decision="pending",
        notes="",
        source_snapshot_path=str(snapshot_path),
        source_snapshot_sha256=snapshot_sha256,
        policy_change_allowed=False,
        auto_apply=False,
        broker_action_allowed=False,
        broker_action_taken=False,
        production_effect="none",
        previous_event_sha256=_text(events[-1].get("event_sha256")) if events else "GENESIS",
    )
    updated_events = [*events, event]
    records = _materialize_rule_owner_decisions(updated_events)
    record = _select_owner_decision(records, decision_id=decision_id, latest=False)
    _write_rule_owner_decision_views(
        journal_path=journal_path,
        events=updated_events,
        records=records,
        record=record,
        source_snapshot=source_snapshot,
    )
    _update_latest_pointer(
        "latest_rule_owner_decision",
        decision_id,
        journal_path,
    )
    return {
        "decision_id": decision_id,
        "journal_path": journal_path,
        "record": record,
        "records": records,
        "event": event,
        "source_snapshot": source_snapshot,
        "decision_dir": decision_dir,
    }


def record_rule_owner_decision(
    *,
    decision_id: str,
    decision: Literal[
        "continue_tracking",
        "keep_current_rules",
        "request_more_data",
        "approve_manual_policy_review",
        "reject_rule_change",
        "defer",
    ],
    notes: str = "",
    journal_path: Path = DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    if decision not in FINAL_OWNER_DECISIONS:
        raise DynamicV3ConfirmationCycleError(f"unsupported owner decision: {decision}")
    generated = _rule_owner_decision_generated_at(generated_at)
    events = _rule_owner_decision_events_for_write(journal_path)
    records = _materialize_rule_owner_decisions(events)
    record = _select_owner_decision(records, decision_id=decision_id, latest=False)
    if record.get("owner_decision") != "pending":
        raise DynamicV3ConfirmationCycleError(
            f"owner decision already finalized: {decision_id}"
        )
    source_snapshot = _read_rule_owner_decision_source_snapshot(
        journal_path=journal_path,
        decision_id=decision_id,
    )
    expected_snapshot = _rule_owner_decision_source_snapshot(
        cycle_id=_text(source_snapshot.get("cycle_id")),
        cycle_root=Path(_text(source_snapshot.get("cycle_root"))),
        captured_at=_rule_owner_decision_timestamp(
            source_snapshot.get("captured_at"),
            field="captured_at",
        ),
    )
    if source_snapshot != expected_snapshot:
        raise DynamicV3ConfirmationCycleError(
            f"rule review source drifted before owner decision: {decision_id}"
        )
    expected_snapshot_path = (
        journal_path.parent
        / decision_id
        / "rule_owner_decision_source_snapshot.json"
    )
    if (
        _texts(record.get("target_ids")) != _texts(source_snapshot.get("target_ids"))
        or _text(record.get("decision_scope_reason"))
        != _text(source_snapshot.get("decision_scope_reason"))
        or _texts(record.get("allowed_owner_decisions"))
        != _texts(source_snapshot.get("allowed_owner_decisions"))
        or _text(record.get("source_snapshot_path")) != str(expected_snapshot_path)
        or _text(record.get("source_snapshot_sha256"))
        != _rule_owner_decision_payload_sha256(source_snapshot)
    ):
        raise DynamicV3ConfirmationCycleError(
            f"owner decision scope/source binding invalid: {decision_id}"
        )
    allowed = tuple(_texts(source_snapshot.get("allowed_owner_decisions")))
    if decision not in allowed:
        raise DynamicV3ConfirmationCycleError(
            f"owner decision not eligible for cycle evidence: {decision}"
        )
    previous_at = _rule_owner_decision_timestamp(
        events[-1].get("event_at"),
        field="event_at",
    )
    if generated <= previous_at:
        raise DynamicV3ConfirmationCycleError(
            "owner decision event time must be strictly later than the journal head"
        )
    event = _rule_owner_decision_event(
        event_type="DECISION_RECORDED",
        decision_id=decision_id,
        cycle_id=_text(record.get("cycle_id")),
        event_at=generated.isoformat(),
        owner_decision=decision,
        notes=notes,
        source_snapshot_path=_text(record.get("source_snapshot_path")),
        source_snapshot_sha256=_text(record.get("source_snapshot_sha256")),
        policy_change_allowed=False,
        auto_apply=False,
        broker_action_allowed=False,
        broker_action_taken=False,
        production_effect="none",
        previous_event_sha256=_text(events[-1].get("event_sha256")),
    )
    updated_events = [*events, event]
    updated_records = _materialize_rule_owner_decisions(updated_events)
    updated_record = _select_owner_decision(
        updated_records,
        decision_id=decision_id,
        latest=False,
    )
    _write_rule_owner_decision_views(
        journal_path=journal_path,
        events=updated_events,
        records=updated_records,
        record=updated_record,
        source_snapshot=source_snapshot,
    )
    _update_latest_pointer(
        "latest_rule_owner_decision",
        decision_id,
        journal_path,
    )
    return {
        "decision_id": decision_id,
        "journal_path": journal_path,
        "record": updated_record,
        "records": updated_records,
        "event": event,
    }


def list_rule_owner_decisions(
    *, journal_path: Path = DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH
) -> dict[str, Any]:
    rows = _read_owner_decision_journal_rows(journal_path)
    legacy = _legacy_rule_owner_decision_journal(rows)
    records = rows if legacy else _materialize_rule_owner_decisions(rows)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_rule_owner_decision_list",
        "status": "PASS_WITH_WARNINGS" if legacy else ("PASS" if records else "MISSING"),
        "journal_path": str(journal_path),
        "decision_count": len(records),
        "pending_count": sum(1 for row in records if row.get("owner_decision") == "pending"),
        "records": records,
        "event_count": 0 if legacy else len(rows),
        "legacy_unsnapshotted": legacy,
        "production_effect": "none",
        "broker_action_allowed": False,
        "auto_apply": False,
    }


def rule_owner_decision_report_payload(
    *,
    decision_id: str | None = None,
    latest: bool = False,
    journal_path: Path = DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH,
) -> dict[str, Any]:
    rows = _read_owner_decision_journal_rows(journal_path)
    if not rows:
        raise DynamicV3ConfirmationCycleError(f"owner decision journal not found: {journal_path}")
    legacy = _legacy_rule_owner_decision_journal(rows)
    records = rows if legacy else _materialize_rule_owner_decisions(rows)
    record = _select_owner_decision(records, decision_id=decision_id, latest=latest)
    report_path = (
        journal_path.parent / "rule_owner_decision_report.md"
        if legacy
        else journal_path.parent
        / _text(record.get("decision_id"))
        / "rule_owner_decision_report.md"
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_rule_owner_decision_report",
        "status": "PASS_WITH_WARNINGS" if legacy else "PASS",
        "decision_id": record.get("decision_id"),
        "owner_decision": record.get("owner_decision"),
        "record": record,
        "records": records,
        "journal_path": str(journal_path),
        "rule_owner_decision_report_path": str(report_path),
        "rule_owner_decision_report": _read_text(report_path) if report_path.exists() else "",
        "global_rule_owner_decision_report_path": str(
            journal_path.parent / "rule_owner_decision_report.md"
        ),
        "legacy_unsnapshotted": legacy,
        "auto_apply": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def validate_rule_owner_decision_artifact(
    *,
    decision_id: str,
    journal_path: Path = DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH,
) -> dict[str, Any]:
    rows = _read_owner_decision_journal_rows(journal_path)
    legacy = _legacy_rule_owner_decision_journal(rows)
    chain_error = ""
    source_error = ""
    records: list[dict[str, Any]] = []
    record: dict[str, Any] = {}
    source_snapshot: dict[str, Any] = {}
    expected_snapshot: dict[str, Any] = {}
    expected_record: dict[str, Any] = {}
    expected_manifest: dict[str, Any] = {}
    expected_detail_report = ""
    expected_global_report = ""
    try:
        if legacy:
            raise DynamicV3ConfirmationCycleError("legacy owner decision journal is unsnapshotted")
        records = _materialize_rule_owner_decisions(rows)
        record = _select_owner_decision(records, decision_id=decision_id, latest=False)
    except (OSError, TypeError, ValueError) as exc:
        chain_error = str(exc)
    if record:
        try:
            source_snapshot = _read_rule_owner_decision_source_snapshot(
                journal_path=journal_path,
                decision_id=decision_id,
            )
            expected_snapshot = _rule_owner_decision_source_snapshot(
                cycle_id=_text(source_snapshot.get("cycle_id")),
                cycle_root=Path(_text(source_snapshot.get("cycle_root"))),
                captured_at=_rule_owner_decision_timestamp(
                    source_snapshot.get("captured_at"),
                    field="captured_at",
                ),
            )
            expected_record = _select_owner_decision(
                _materialize_rule_owner_decisions(rows),
                decision_id=decision_id,
                latest=False,
            )
            expected_manifest = _rule_owner_decision_manifest(expected_record)
            expected_detail_report = render_rule_owner_decision_detail_report(
                expected_record,
                expected_snapshot,
            )
            expected_global_report = render_rule_owner_decision_report(records)
        except (OSError, TypeError, ValueError) as exc:
            source_error = str(exc)
    decision_dir = journal_path.parent / decision_id
    record_path = decision_dir / "rule_owner_decision_record.json"
    manifest_path = decision_dir / "rule_owner_decision_manifest.json"
    snapshot_path = decision_dir / "rule_owner_decision_source_snapshot.json"
    detail_report_path = decision_dir / "rule_owner_decision_report.md"
    global_report_path = journal_path.parent / "rule_owner_decision_report.md"
    checks = [
        _check("journal_exists", journal_path.exists(), str(journal_path)),
        _check("journal_schema_current", not legacy, "legacy_unsnapshotted=false"),
        _check("event_chain_valid", not chain_error, chain_error),
        _check("decision_exists", bool(record), decision_id),
        _check(
            "owner_decision_valid",
            _text(record.get("owner_decision")) in OWNER_DECISIONS,
            "owner_decision",
        ),
        _check("auto_apply_forbidden", record.get("auto_apply") is False, "auto_apply=false"),
        _check(
            "broker_action_forbidden",
            record.get("broker_action_allowed") is False
            and record.get("broker_action_taken") is False,
            "broker action",
        ),
        _check(
            "production_effect_none",
            record.get("production_effect") == "none",
            "production_effect=none",
        ),
        _check(
            "policy_change_disallowed",
            record.get("policy_change_allowed") is False,
            "policy_change_allowed=false",
        ),
        _check("source_snapshot_valid", not source_error, source_error),
        _check(
            "source_snapshot_recomputed",
            bool(source_snapshot) and source_snapshot == expected_snapshot,
            "live rule review cycle",
        ),
        _check(
            "source_snapshot_hash_valid",
            bool(source_snapshot)
            and _text(record.get("source_snapshot_sha256"))
            == _rule_owner_decision_payload_sha256(source_snapshot),
            "source snapshot sha256",
        ),
        _check(
            "decision_eligible",
            _text(record.get("owner_decision")) == "pending"
            or _text(record.get("owner_decision"))
            in _texts(source_snapshot.get("allowed_owner_decisions")),
            "allowed owner decisions",
        ),
        _check(
            "decision_scope_bound_to_snapshot",
            _texts(record.get("target_ids")) == _texts(source_snapshot.get("target_ids"))
            and _text(record.get("decision_scope_reason"))
            == _text(source_snapshot.get("decision_scope_reason"))
            and _texts(record.get("allowed_owner_decisions"))
            == _texts(source_snapshot.get("allowed_owner_decisions"))
            and _text(record.get("source_snapshot_path")) == str(snapshot_path),
            "snapshot scope/path",
        ),
        _check(
            "record_recomputed",
            record_path.is_file() and _read_optional_json(record_path) == expected_record,
            "materialized record",
        ),
        _check(
            "manifest_recomputed",
            manifest_path.is_file()
            and _read_optional_json(manifest_path) == expected_manifest,
            "materialized manifest",
        ),
        _check(
            "json_bytes_recomputed",
            all(
                path.is_file() and _read_text(path) == _rule_owner_decision_json_text(payload)
                for path, payload in (
                    (snapshot_path, expected_snapshot),
                    (record_path, expected_record),
                    (manifest_path, expected_manifest),
                )
            ),
            "canonical JSON bytes",
        ),
        _check(
            "journal_bytes_recomputed",
            journal_path.is_file()
            and _read_text(journal_path) == _rule_owner_decision_jsonl_text(rows),
            "canonical event JSONL bytes",
        ),
        _check(
            "detail_report_recomputed",
            detail_report_path.is_file()
            and _read_text(detail_report_path) == expected_detail_report,
            "per-decision report bytes",
        ),
        _check(
            "global_report_recomputed",
            global_report_path.is_file()
            and _read_text(global_report_path) == expected_global_report,
            "global report bytes",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_rule_owner_decision_validation",
        artifact_id_key="decision_id",
        artifact_id=decision_id,
        checks=checks,
    )


def render_confirmation_registry_report(
    manifest: Mapping[str, Any], registry: Mapping[str, Any]
) -> str:
    targets = _records(registry.get("targets"))
    lines = [
        "# Dynamic Rescue Forward Confirmation Target Registry",
        "",
        f"- registry_id: `{manifest.get('registry_id')}`",
        f"- source_confirmation_plan_id: `{manifest.get('source_confirmation_plan_id')}`",
        "- market_regime: `ai_after_chatgpt`",
        f"- targets_total: {len(targets)}",
        f"- active_target_count: {manifest.get('active_target_count')}",
        f"- watch_only_target_count: {manifest.get('watch_only_target_count')}",
        "- auto_apply: `false`",
        "- owner_approval_required: `true`",
        "- production_effect: `none`",
        "",
        "## Targets",
    ]
    for row in targets:
        lines.extend(
            [
                "",
                f"### {_text(row.get('target_id'))}",
                f"- status: `{row.get('status')}`",
                f"- current_status: `{row.get('current_status')}`",
                f"- priority: `{row.get('priority')}`",
                f"- windows: `{row.get('windows')}`",
                f"- required_forward_events: `{row.get('required_forward_events', '')}`",
                f"- required_pressure_regime_events: "
                f"`{row.get('required_pressure_regime_events', '')}`",
            ]
        )
    return "\n".join(lines) + "\n"


def render_confirmation_progress_report(
    manifest: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic Rescue Confirmation Progress",
        "",
        f"- progress_id: `{manifest.get('progress_id')}`",
        f"- registry_id: `{manifest.get('registry_id')}`",
        "- market_regime: `ai_after_chatgpt`",
        f"- ready_for_evaluation_count: {summary.get('ready_for_evaluation_count')}",
        f"- insufficient_events_count: {summary.get('insufficient_events_count')}",
        f"- summary_recommendation: `{summary.get('summary_recommendation')}`",
        "- auto_apply: `false`",
        "- production_effect: `none`",
        "",
        "## Target Progress",
    ]
    for row in rows:
        lines.extend(
            [
                "",
                f"### {_text(row.get('target_id'))}",
                f"- progress_status: `{row.get('progress_status')}`",
                f"- available_forward_events: {row.get('available_forward_events')}",
                f"- required_forward_events: {row.get('required_forward_events')}",
                f"- blocking_reasons: `{row.get('blocking_reasons')}`",
            ]
        )
    return "\n".join(lines) + "\n"


def render_confirmation_evaluation_report(
    manifest: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic Rescue Confirmation Evaluation",
        "",
        f"- evaluation_id: `{manifest.get('evaluation_id')}`",
        f"- progress_id: `{manifest.get('progress_id')}`",
        f"- success_count: {summary.get('success_count')}",
        f"- failure_count: {summary.get('failure_count')}",
        f"- not_ready_count: {summary.get('not_ready_count')}",
        f"- rule_review_ready: `{summary.get('rule_review_ready')}`",
        f"- summary_recommendation: `{summary.get('summary_recommendation')}`",
        "- auto_apply: `false`",
        "- production_effect: `none`",
        "",
        "## Target Evaluations",
    ]
    for row in rows:
        lines.extend(
            [
                "",
                f"### {_text(row.get('target_id'))}",
                f"- evaluation_status: `{row.get('evaluation_status')}`",
                f"- progress_status: `{row.get('progress_status')}`",
                f"- failure_conditions_triggered: `{row.get('failure_conditions_triggered')}`",
                f"- recommendation: `{row.get('recommendation')}`",
            ]
        )
    return "\n".join(lines) + "\n"


def render_rule_review_cycle_report(
    manifest: Mapping[str, Any], decision_matrix: Mapping[str, Any]
) -> str:
    targets = _records(decision_matrix.get("targets"))
    progress_ready_count = sum(
        1 for row in targets if row.get("progress_status") == "READY_FOR_EVALUATION"
    )
    success_count = sum(1 for row in targets if row.get("current_status") == "SUCCESS")
    failure_count = sum(1 for row in targets if row.get("current_status") == "FAILURE")
    mixed_count = sum(1 for row in targets if row.get("current_status") == "MIXED")
    review_required_count = sum(
        1 for row in targets if row.get("current_status") == "REVIEW_REQUIRED"
    )
    lines = [
        "# Dynamic Rescue Rule Review Cycle",
        "",
        f"- cycle_id: `{manifest.get('cycle_id')}`",
        f"- registry_id: `{manifest.get('registry_id')}`",
        f"- progress_id: `{manifest.get('progress_id')}`",
        f"- evaluation_id: `{manifest.get('evaluation_id')}`",
        f"- cycle_recommendation: `{manifest.get('cycle_recommendation')}`",
        "- policy_change_allowed: `false`",
        "- auto_apply: `false`",
        "- production_effect: `none`",
        "",
        "## Review Questions",
        "",
        f"- active targets: {sum(1 for row in targets if row.get('target_status') == 'active')}",
        f"- progress ready for evaluation: {progress_ready_count}",
        f"- evaluation success: {success_count}",
        f"- evaluation failure: {failure_count}",
        f"- evaluation mixed: {mixed_count}",
        f"- evaluation review required: {review_required_count}",
        "- policy change allowed: false",
        "",
        "## Decision Matrix",
    ]
    for row in targets:
        lines.extend(
            [
                "",
                f"### {_text(row.get('target_id'))}",
                f"- progress_status: `{row.get('progress_status')}`",
                f"- current_status: `{row.get('current_status')}`",
                f"- rule_review_decision: `{row.get('rule_review_decision')}`",
                f"- owner_action_required: `{row.get('owner_action_required')}`",
                f"- evaluation_recommendation: `{row.get('evaluation_recommendation')}`",
                f"- criteria_results: `{row.get('criteria_results')}`",
                f"- failure_conditions: `{row.get('failure_conditions')}`",
                f"- failure_conditions_triggered: `{row.get('failure_conditions_triggered')}`",
                f"- policy_change_allowed: `{row.get('policy_change_allowed')}`",
                f"- reason: {row.get('reason')}",
            ]
        )
    return "\n".join(lines) + "\n"


def render_rule_review_reader_brief(decision_matrix: Mapping[str, Any]) -> str:
    targets = _records(decision_matrix.get("targets"))
    progress_ready_count = sum(
        1 for row in targets if row.get("progress_status") == "READY_FOR_EVALUATION"
    )
    success_count = sum(1 for row in targets if row.get("current_status") == "SUCCESS")
    failure_count = sum(1 for row in targets if row.get("current_status") == "FAILURE")
    mixed_count = sum(1 for row in targets if row.get("current_status") == "MIXED")
    review_required_count = sum(
        1 for row in targets if row.get("current_status") == "REVIEW_REQUIRED"
    )
    owner_count = sum(1 for row in targets if row.get("owner_action_required") is True)
    return (
        "## Dynamic Rescue Rule Review Cycle\n\n"
        f"- targets_total: {len(targets)}\n"
        f"- progress_ready_for_evaluation_count: {progress_ready_count}\n"
        f"- evaluation_success_count: {success_count}\n"
        f"- evaluation_failure_count: {failure_count}\n"
        f"- evaluation_mixed_count: {mixed_count}\n"
        f"- evaluation_review_required_count: {review_required_count}\n"
        f"- rule_review_decision: `{decision_matrix.get('cycle_recommendation')}`\n"
        f"- cycle_recommendation: `{decision_matrix.get('cycle_recommendation')}`\n"
        f"- targets_requiring_owner_action: {owner_count}\n"
        "- next_action: `continue_forward_tracking_or_owner_manual_review`\n"
        "- policy_change_allowed: `false`\n"
        "- production_effect: `none`\n"
    )


def render_rule_owner_decision_report(records: Sequence[Mapping[str, Any]]) -> str:
    pending = [row for row in records if row.get("owner_decision") == "pending"]
    latest = records[-1] if records else {}
    lines = [
        "# Dynamic Rescue Rule Owner Decision Journal",
        "",
        f"- decision_count: {len(records)}",
        f"- pending_count: {len(pending)}",
        f"- latest_decision_id: `{latest.get('decision_id', '')}`",
        f"- latest_owner_decision: `{latest.get('owner_decision', '')}`",
        "- auto_apply: `false`",
        "- broker_action_allowed: `false`",
        "- production_effect: `none`",
        "",
        "## Records",
    ]
    for row in records:
        lines.extend(
            [
                "",
                f"### {_text(row.get('decision_id'))}",
                f"- cycle_id: `{row.get('cycle_id')}`",
                f"- owner_decision: `{row.get('owner_decision')}`",
                f"- target_ids: `{row.get('target_ids')}`",
                f"- recommended_cycle_action: `{row.get('recommended_cycle_action')}`",
                f"- notes: {row.get('notes', '')}",
            ]
        )
    return "\n".join(lines) + "\n"


def render_rule_owner_decision_detail_report(
    record: Mapping[str, Any],
    source_snapshot: Mapping[str, Any],
) -> str:
    return (
        "# Dynamic Rescue Rule Owner Decision\n\n"
        f"- decision_id: `{record.get('decision_id')}`\n"
        f"- cycle_id: `{record.get('cycle_id')}`\n"
        f"- status: `{'PENDING' if record.get('owner_decision') == 'pending' else 'RECORDED'}`\n"
        f"- owner_decision: `{record.get('owner_decision')}`\n"
        f"- target_ids: `{record.get('target_ids')}`\n"
        f"- decision_scope_reason: `{record.get('decision_scope_reason')}`\n"
        f"- recommended_cycle_action: `{record.get('recommended_cycle_action')}`\n"
        f"- allowed_owner_decisions: `{record.get('allowed_owner_decisions')}`\n"
        f"- ready_for_owner_review_target_ids: "
        f"`{source_snapshot.get('ready_for_owner_review_target_ids')}`\n"
        f"- event_count: {record.get('event_count')}\n"
        f"- source_snapshot_sha256: `{record.get('source_snapshot_sha256')}`\n"
        f"- notes: {record.get('notes', '')}\n"
        "- manual_policy_review_only: `true`\n"
        "- policy_change_allowed: `false`\n"
        "- auto_apply: `false`\n"
        "- broker_action_allowed: `false`\n"
        "- production_effect: `none`\n"
    )


def _registry_target(
    row: Mapping[str, Any], failure_conditions: Mapping[str, Any]
) -> dict[str, Any]:
    target_id = _text(row.get("target_id"))
    status = "watch_only" if target_id == "consensus_target_risk" else "active"
    current_status = "watch_only" if status == "watch_only" else "in_progress"
    failure_map = {
        _text(item.get("condition")): {"action": _text(item.get("action"))}
        for item in _records(failure_conditions.get("failure_conditions"))
        if _text(item.get("target")) == target_id
    }
    result = {
        "target_id": target_id,
        "status": status,
        "priority": _text(row.get("priority"), "MEDIUM"),
        "source": "backtest_sim_forward_confirmation_plan",
        "windows": [_int(item) for item in row.get("windows", [])],
        "success_criteria": _mapping(row.get("success_criteria")),
        "failure_conditions": failure_map,
        "current_status": current_status,
        "owner_approval_required": True,
        "auto_apply": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }
    if "required_pressure_regime_events" in row:
        result["required_pressure_regime_events"] = _int(row.get("required_pressure_regime_events"))
    if "required_forward_events" in row:
        result["required_forward_events"] = _int(row.get("required_forward_events"))
    return result


def _progress_row_v2(
    target: Mapping[str, Any],
    *,
    source_kind: str,
    source_bundle: Mapping[str, Any],
    source_missing: bool,
    generated: datetime,
) -> dict[str, Any]:
    target_id = _text(target.get("target_id"))
    windows = _strict_progress_windows(target)
    required_key = (
        "required_pressure_regime_events"
        if target_id == "defensive_limited_adjustment_drawdown"
        else "required_forward_events"
    )
    required_raw = target.get(required_key)
    required = _int(required_raw)
    if isinstance(required_raw, bool) or required <= 0:
        raise DynamicV3ConfirmationCycleError(f"{target_id} missing positive {required_key}")
    available_by_window = {window: 0 for window in windows}
    metrics: dict[str, Any]
    blocking: list[str] = []
    if source_missing:
        metrics = {
            "aggregation_status": "INSUFFICIENT_DATA",
            "metrics_by_window": [],
        }
        blocking.append(f"missing_{source_kind}_artifact")
    elif source_kind == "limited_vs_notrade":
        json_views = _mapping(source_bundle.get("json"))
        samples = _records(_mapping(source_bundle.get("jsonl")).get("sample_inventory.jsonl"))
        identities: set[tuple[str, int]] = set()
        for row in samples:
            sample_id = _text(row.get("sample_id"))
            window = _int(row.get("window_days"))
            identity = (sample_id, window)
            if not sample_id or identity in identities:
                raise DynamicV3ConfirmationCycleError("limited progress sample identity invalid")
            identities.add(identity)
            if (
                window in available_by_window
                and row.get("sample_status") == "AVAILABLE"
                and _finite_or_none(row.get("relative_return")) is not None
            ):
                available_by_window[window] += 1
        metric_rows = [
            dict(row)
            for row in _records(
                _mapping(json_views.get("window_comparison_metrics.json")).get("by_window")
            )
            if _int(row.get("window_days")) in set(windows)
        ]
        if target_id == "defensive_limited_adjustment_drawdown":
            available_by_window = {window: 0 for window in windows}
            metrics = {
                "aggregation_status": "INSUFFICIENT_PRESSURE_REGIME_TAGS",
                "metrics_by_window": metric_rows,
                "drawdown_delta_vs_no_trade": None,
                "win_rate_vs_no_trade": None,
                "pressure_regime_sample_status": "MISSING_PRESSURE_REGIME_TAGS",
            }
            blocking.extend(
                [
                    "missing_pressure_regime_forward_events",
                    "pressure_regime_tagged_outcomes_required",
                ]
            )
        else:
            metrics = {
                "aggregation_status": "WINDOW_METRICS_ONLY",
                "metrics_by_window": metric_rows,
                "win_rate_vs_no_trade": None,
                "avg_relative_return": None,
                "drawdown_delta": None,
            }
    elif source_kind == "consensus_risk":
        json_views = _mapping(source_bundle.get("json"))
        pairs = _records(_mapping(source_bundle.get("jsonl")).get("consensus_drawdown_pairs.jsonl"))
        identities: set[tuple[str, int]] = set()
        for row in pairs:
            sample_id = _text(row.get("sample_id") or row.get("event_id"))
            window = _int(row.get("window_days"))
            identity = (sample_id, window)
            if not sample_id or identity in identities:
                raise DynamicV3ConfirmationCycleError("consensus progress sample identity invalid")
            identities.add(identity)
            if (
                window in available_by_window
                and row.get("sample_status") == "AVAILABLE"
                and _finite_or_none(row.get("drawdown_delta_vs_no_trade")) is not None
            ):
                available_by_window[window] += 1
        metrics = {
            "aggregation_status": "WINDOW_METRICS_ONLY",
            "metrics_by_window": _records(
                _mapping(json_views.get("consensus_drawdown_risk.json")).get("window_results")
            ),
            "drawdown_delta_vs_limited_adjustment": None,
            "drawdown_delta_vs_no_trade": None,
            "turnover_delta": None,
            "consensus_target_risk": _mapping(json_views.get("consensus_risk_manifest.json")).get(
                "consensus_target_risk"
            ),
        }
        blocking.append("missing_consensus_vs_limited_adjustment_drawdown_metric")
    else:
        raise DynamicV3ConfirmationCycleError(f"unsupported progress source kind: {source_kind}")
    available = max(available_by_window.values(), default=0)
    if available < required:
        blocking.append("not_enough_forward_events")
    missing_windows = [str(window) for window, count in available_by_window.items() if count <= 0]
    if missing_windows:
        blocking.append("missing_" + "_".join(missing_windows) + "d_windows")
    status = _progress_status_v2(
        available=available,
        required=required,
        available_by_window=available_by_window,
        blocking=blocking,
    )
    return {
        "target_id": target_id,
        "status": target.get("current_status"),
        "target_status": target.get("status"),
        "priority": target.get("priority"),
        "windows": windows,
        "required_forward_events": (
            required if required_key == "required_forward_events" else None
        ),
        "required_pressure_regime_events": (
            required if required_key == "required_pressure_regime_events" else None
        ),
        "available_forward_events": (available if required_key == "required_forward_events" else 0),
        "available_pressure_regime_events": (
            available if required_key == "required_pressure_regime_events" else 0
        ),
        "available_event_count_unit": "unique_event_conservative_max_across_windows",
        "available_by_window": {str(key): value for key, value in available_by_window.items()},
        "current_metrics": metrics,
        "success_criteria": dict(_mapping(target.get("success_criteria"))),
        "failure_conditions": list(_records(target.get("failure_conditions"))),
        "progress_status": status,
        "blocking_reasons": sorted(set(blocking)),
        "source_kind": source_kind,
        "source_status": "MISSING" if source_missing else "VALIDATED",
        "last_updated": generated.isoformat(),
        "owner_approval_required": True,
        "auto_apply": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _strict_progress_windows(target: Mapping[str, Any]) -> list[int]:
    raw = target.get("windows")
    if not isinstance(raw, list) or not raw:
        raise DynamicV3ConfirmationCycleError("confirmation target windows missing")
    windows = [_int(value) for value in raw]
    if any(isinstance(value, bool) for value in raw) or any(value <= 0 for value in windows):
        raise DynamicV3ConfirmationCycleError("confirmation target windows invalid")
    if len(windows) != len(set(windows)):
        raise DynamicV3ConfirmationCycleError("confirmation target windows duplicate")
    return windows


def _finite_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    candidate = _float(value, default=float("nan"))
    return candidate if math.isfinite(candidate) else None


def _progress_status_v2(
    *,
    available: int,
    required: int,
    available_by_window: Mapping[int, int],
    blocking: Sequence[str],
) -> str:
    if any(reason.startswith("invalid_") for reason in blocking):
        return "BLOCKED"
    if (
        available >= required
        and all(count > 0 for count in available_by_window.values())
        and not blocking
    ):
        return "READY_FOR_EVALUATION"
    return "IN_PROGRESS" if available > 0 else "INSUFFICIENT_EVENTS"


def _progress_summary(
    rows: Sequence[Mapping[str, Any]], registry_id: str, generated: datetime
) -> dict[str, Any]:
    ready = sum(1 for row in rows if row.get("progress_status") == "READY_FOR_EVALUATION")
    blocked = sum(1 for row in rows if row.get("progress_status") == "BLOCKED")
    insufficient = sum(1 for row in rows if row.get("progress_status") == "INSUFFICIENT_EVENTS")
    watch_only = sum(1 for row in rows if row.get("target_status") == "watch_only")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_progress_summary",
        "progress_id": "",
        "registry_id": registry_id,
        "generated_at": generated.isoformat(),
        "targets_total": len(rows),
        "ready_for_evaluation_count": ready,
        "in_progress_count": sum(
            1 for row in rows if row.get("progress_status") in {"IN_PROGRESS", "NEAR_READY"}
        ),
        "blocked_count": blocked,
        "insufficient_events_count": insufficient,
        "watch_only_count": watch_only,
        "summary_recommendation": (
            "run_success_failure_evaluation" if ready else "continue_forward_tracking"
        ),
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _evaluation_row_v2(row: Mapping[str, Any], *, generated: datetime) -> dict[str, Any]:
    target_id = _text(row.get("target_id"))
    progress_status = _text(row.get("progress_status"))
    if progress_status not in PROGRESS_STATUSES:
        raise DynamicV3ConfirmationCycleError(f"progress status invalid: {target_id}")
    criteria = _strict_evaluation_criteria(row)
    failures = _strict_failure_conditions(
        target_id=target_id,
        failure_conditions=row.get("failure_conditions"),
        criteria=criteria,
    )
    metrics = _mapping(row.get("current_metrics"))
    if progress_status != "READY_FOR_EVALUATION":
        criteria_results = {
            name: {"required": required, "actual": None, "status": "NOT_EVALUATED"}
            for name, required in criteria.items()
        }
        triggered: list[dict[str, Any]] = []
        status = "NOT_READY"
        recommendation = "continue_tracking"
    else:
        criteria_results = {
            name: _criteria_result_v2(name, required, metrics)
            for name, required in criteria.items()
        }
        triggered = _failure_conditions_triggered_v2(failures, criteria_results)
        statuses = [_text(item.get("status")) for item in criteria_results.values()]
        if triggered or "FAIL" in statuses:
            status = "FAILURE"
            recommendation = _failure_recommendation(row, triggered)
        elif statuses and all(item == "PASS" for item in statuses):
            status = "SUCCESS"
            recommendation = "ready_for_rule_review"
        elif "INSUFFICIENT_DATA" in statuses:
            status = "REVIEW_REQUIRED"
            recommendation = "manual_review_required"
        else:
            status = "MIXED"
            recommendation = "manual_review_required"
    return {
        "target_id": target_id,
        "evaluation_status": status,
        "progress_status": progress_status,
        "criteria_results": criteria_results,
        "failure_conditions": failures,
        "failure_conditions_triggered": triggered,
        "recommendation": recommendation,
        "evaluated_at": generated.isoformat(),
        "owner_approval_required": True,
        "auto_apply": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _strict_evaluation_criteria(row: Mapping[str, Any]) -> dict[str, float]:
    criteria = _mapping(row.get("success_criteria"))
    if not criteria:
        raise DynamicV3ConfirmationCycleError("evaluation success criteria missing")
    result: dict[str, float] = {}
    for name, required in criteria.items():
        key = _text(name)
        finite_required = _finite_or_none(required)
        if not key or not key.endswith(("_min", "_max")) or finite_required is None:
            raise DynamicV3ConfirmationCycleError(f"evaluation criterion invalid: {key}")
        result[key] = finite_required
    return result


def _strict_failure_conditions(
    *,
    target_id: str,
    failure_conditions: Any,
    criteria: Mapping[str, float],
) -> list[dict[str, Any]]:
    rows = _records(failure_conditions)
    if not rows:
        raise DynamicV3ConfirmationCycleError(f"failure conditions missing: {target_id}")
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for row in rows:
        condition = _text(row.get("condition"))
        action = _text(row.get("action"))
        source_target = _text(row.get("target"), target_id)
        candidates = FAILURE_CONDITION_CRITERION_KEYS.get(condition)
        criterion = next(
            (candidate for candidate in candidates or () if candidate in criteria),
            "",
        )
        if (
            not condition
            or condition in seen
            or not action
            or source_target != target_id
            or candidates is None
            or not criterion
        ):
            raise DynamicV3ConfirmationCycleError(
                f"failure condition/criterion binding invalid: {target_id}:{condition}"
            )
        seen.add(condition)
        result.append(
            {
                "target": target_id,
                "condition": condition,
                "action": action,
                "criterion": criterion,
                "required_boundary": criteria[criterion],
            }
        )
    return result


def _criteria_result_v2(name: str, required: float, metrics: Mapping[str, Any]) -> dict[str, Any]:
    actual = _finite_or_none(_metric_actual(name, metrics))
    if actual is None:
        status = "INSUFFICIENT_DATA"
    elif name.endswith("_min"):
        status = "PASS" if actual >= required else "FAIL"
    else:
        status = "PASS" if actual <= required else "FAIL"
    return {"required": required, "actual": actual, "status": status}


def _failure_conditions_triggered_v2(
    failure_conditions: Sequence[Mapping[str, Any]],
    criteria_results: Mapping[str, Any],
) -> list[dict[str, Any]]:
    return [
        {
            "condition": row.get("condition"),
            "action": row.get("action"),
            "criterion": row.get("criterion"),
            "required_boundary": row.get("required_boundary"),
            "actual": _mapping(criteria_results.get(_text(row.get("criterion")))).get("actual"),
        }
        for row in failure_conditions
        if _mapping(criteria_results.get(_text(row.get("criterion")))).get("status") == "FAIL"
    ]


def _evaluation_summary(
    rows: Sequence[Mapping[str, Any]], progress_id: str, generated: datetime
) -> dict[str, Any]:
    counts = {
        status: sum(1 for row in rows if row.get("evaluation_status") == status)
        for status in EVALUATION_STATUSES
    }
    review_ready = counts["SUCCESS"] > 0 or counts["FAILURE"] > 0 or counts["REVIEW_REQUIRED"] > 0
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_evaluation_summary",
        "evaluation_id": "",
        "progress_id": progress_id,
        "generated_at": generated.isoformat(),
        "success_count": counts["SUCCESS"],
        "failure_count": counts["FAILURE"],
        "mixed_count": counts["MIXED"],
        "not_ready_count": counts["NOT_READY"],
        "review_required_count": counts["REVIEW_REQUIRED"],
        "rule_review_ready": review_ready,
        "summary_recommendation": (
            "prepare_rule_review_cycle" if review_ready else "continue_forward_tracking"
        ),
        "auto_apply": False,
        "production_effect": "none",
    }


def _rule_review_decision_row(
    target: Mapping[str, Any],
    *,
    progress: Mapping[str, Any],
    evaluation: Mapping[str, Any],
) -> dict[str, Any]:
    target_id = _text(target.get("target_id"))
    evaluation_status = _text(evaluation.get("evaluation_status"), "NOT_READY")
    failure_conditions = _records(evaluation.get("failure_conditions"))
    failures = _records(evaluation.get("failure_conditions_triggered"))
    criteria_results = dict(_mapping(evaluation.get("criteria_results")))
    if target.get("status") == "watch_only":
        decision = "KEEP_REFERENCE_ONLY"
        reason = "The source registry classifies this target as watch-only reference evidence."
        owner_action = False
    elif evaluation_status in {"SUCCESS", "FAILURE"}:
        decision = "READY_FOR_OWNER_REVIEW"
        reason = (
            "Evaluation is complete; source criteria and failure actions are ready for manual "
            "owner review without authorizing a rule change."
        )
        owner_action = True
    elif evaluation_status in {"MIXED", "REVIEW_REQUIRED"}:
        decision = "DEFER"
        reason = "Evidence is mixed or incomplete; defer rule mutation."
        owner_action = False
    else:
        decision = "CONTINUE_TRACKING"
        available = _available_event_count(progress)
        required = _required_event_count(progress)
        reason = f"Only {available} eligible events are available; {required} required."
        owner_action = False
    return {
        "target_id": target_id,
        "target_status": target.get("status"),
        "current_status": evaluation_status,
        "progress_status": progress.get("progress_status"),
        "rule_review_decision": decision,
        "reason": reason,
        "criteria_results": criteria_results,
        "failure_conditions": failure_conditions,
        "failure_conditions_triggered": failures,
        "evaluation_recommendation": evaluation.get("recommendation"),
        "owner_action_required": owner_action,
        "policy_change_allowed": False,
        "auto_apply": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _cycle_recommendation(rows: Sequence[Mapping[str, Any]]) -> str:
    if any(row.get("owner_action_required") is True for row in rows):
        return "owner_manual_review_required"
    if any(row.get("rule_review_decision") == "DEFER" for row in rows):
        return "defer_until_more_evidence"
    return "continue_tracking"


def _metric_actual(name: str, metrics: Mapping[str, Any]) -> Any:
    mapping = {
        "win_rate_vs_no_trade_min": "win_rate_vs_no_trade",
        "avg_relative_return_min": "avg_relative_return",
        "drawdown_delta_max": "drawdown_delta",
        "drawdown_delta_vs_no_trade_max": "drawdown_delta_vs_no_trade",
        "drawdown_delta_vs_limited_adjustment_max": "drawdown_delta_vs_limited_adjustment",
        "turnover_delta_max": "turnover_delta",
    }
    key = mapping.get(name, name.removesuffix("_min").removesuffix("_max"))
    return metrics.get(key)


def _failure_recommendation(row: Mapping[str, Any], triggered: Sequence[Mapping[str, Any]]) -> str:
    target_id = _text(row.get("target_id"))
    actions = {_text(item.get("action")) for item in triggered}
    if "tighten_or_disable_limited_adjustment_proposal" in actions:
        return "tighten_rules"
    if target_id == "defensive_limited_adjustment_drawdown":
        return "do_not_loosen_rules"
    return "manual_review_required"


def _required_event_count(row: Mapping[str, Any]) -> int:
    pressure = _int(row.get("required_pressure_regime_events"))
    forward = _int(row.get("required_forward_events"))
    return pressure or forward


def _available_event_count(row: Mapping[str, Any]) -> int:
    pressure = _int(row.get("available_pressure_regime_events"))
    forward = _int(row.get("available_forward_events"))
    return pressure or forward


def _rule_owner_decision_generated_at(value: datetime | None) -> datetime:
    generated = value or datetime.now(UTC)
    if generated.tzinfo is None or generated.utcoffset() is None:
        raise DynamicV3ConfirmationCycleError("generated_at must be timezone-aware")
    return generated.astimezone(UTC)


def _rule_owner_decision_timestamp(value: Any, *, field: str) -> datetime:
    parsed = value if isinstance(value, datetime) else _datetime_from_any(value)
    if parsed is None or parsed.tzinfo is None or parsed.utcoffset() is None:
        raise DynamicV3ConfirmationCycleError(
            f"owner decision {field} must be timezone-aware"
        )
    return parsed.astimezone(UTC)


def _rule_owner_decision_cycle_bundle(cycle_dir: Path) -> dict[str, Any]:
    json_files = (
        "rule_review_cycle_manifest.json",
        "rule_review_decision_matrix.json",
        "rule_review_cycle_input_snapshot.json",
    )
    text_files = ("rule_review_cycle_report.md", "reader_brief_section.md")
    return _bounded_source_bundle(
        source_dir=cycle_dir,
        canonical_files=json_files + text_files,
        json_views=(
            "rule_review_cycle_manifest.json",
            "rule_review_decision_matrix.json",
        ),
    )


def _rule_owner_decision_source_snapshot(
    *,
    cycle_id: str,
    cycle_root: Path,
    captured_at: datetime,
) -> dict[str, Any]:
    captured = _rule_owner_decision_generated_at(captured_at)
    validation = validate_rule_review_cycle_artifact(
        cycle_id=cycle_id,
        output_dir=cycle_root,
    )
    if validation.get("status") != "PASS":
        raise DynamicV3ConfirmationCycleError(
            f"rule review cycle validation failed: {cycle_id}"
        )
    bundle = _rule_owner_decision_cycle_bundle(cycle_root / cycle_id)
    json_views = _mapping(bundle.get("json"))
    manifest = _mapping(json_views.get("rule_review_cycle_manifest.json"))
    matrix = _mapping(json_views.get("rule_review_decision_matrix.json"))
    if manifest.get("cycle_id") != cycle_id or matrix.get("cycle_id") != cycle_id:
        raise DynamicV3ConfirmationCycleError(
            f"rule review cycle id mismatch: {cycle_id}"
        )
    cycle_generated = _rule_owner_decision_timestamp(
        manifest.get("generated_at"),
        field="cycle generated_at",
    )
    if cycle_generated > captured:
        raise DynamicV3ConfirmationCycleError(
            f"rule review cycle is newer than owner decision cutoff: {cycle_id}"
        )
    targets = _records(matrix.get("targets"))
    all_target_ids = _strict_rule_review_target_ids(targets, source_name="rule review")
    ready_targets = [
        row for row in targets if row.get("rule_review_decision") == "READY_FOR_OWNER_REVIEW"
    ]
    owner_targets = [row for row in targets if row.get("owner_action_required") is True]
    ready_target_ids = [_text(row.get("target_id")) for row in ready_targets]
    owner_target_ids = [_text(row.get("target_id")) for row in owner_targets]
    if ready_target_ids != owner_target_ids:
        raise DynamicV3ConfirmationCycleError(
            "rule review owner-action scope does not match READY_FOR_OWNER_REVIEW targets"
        )
    if owner_target_ids:
        target_ids = owner_target_ids
        scope_reason = "OWNER_ACTION_REQUIRED_TARGETS"
    else:
        target_ids = all_target_ids
        scope_reason = "ALL_CYCLE_TARGETS_NO_READY_OWNER_ACTION"
    allowed = list(FINAL_OWNER_DECISIONS)
    if not ready_target_ids:
        allowed.remove("approve_manual_policy_review")
    recommendation = _text(matrix.get("cycle_recommendation"))
    if not recommendation:
        raise DynamicV3ConfirmationCycleError(
            f"rule review cycle recommendation missing: {cycle_id}"
        )
    return {
        "schema_version": RULE_OWNER_DECISION_SOURCE_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_rule_owner_decision_source_snapshot",
        "captured_at": captured.isoformat(),
        "cycle_id": cycle_id,
        "cycle_root": str(cycle_root),
        "cycle_generated_at": cycle_generated.isoformat(),
        "cycle_validation": validation,
        "cycle_bundle": bundle,
        "all_target_ids": all_target_ids,
        "ready_for_owner_review_target_ids": ready_target_ids,
        "target_ids": target_ids,
        "decision_scope_reason": scope_reason,
        "recommended_cycle_action": recommendation,
        "allowed_owner_decisions": allowed,
        "policy_change_allowed": False,
        "auto_apply": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
    }


def _read_owner_decision_journal_rows(path: Path) -> list[dict[str, Any]]:
    return _read_jsonl(path) if path.is_file() else []


def _legacy_rule_owner_decision_journal(rows: Sequence[Mapping[str, Any]]) -> bool:
    return bool(rows) and any(
        row.get("schema_version") != RULE_OWNER_DECISION_EVENT_SCHEMA_VERSION
        or row.get("event_type") not in {"DECISION_CREATED", "DECISION_RECORDED"}
        for row in rows
    )


def _rule_owner_decision_events_for_write(path: Path) -> list[dict[str, Any]]:
    rows = _read_owner_decision_journal_rows(path)
    if _legacy_rule_owner_decision_journal(rows):
        raise DynamicV3ConfirmationCycleError(
            f"legacy owner decision journal is read-only and requires explicit migration: {path}"
        )
    _materialize_rule_owner_decisions(rows)
    return rows


def _rule_owner_decision_event(
    *,
    event_type: str,
    decision_id: str,
    cycle_id: str,
    event_at: str,
    previous_event_sha256: str,
    **fields: Any,
) -> dict[str, Any]:
    event_time = _rule_owner_decision_timestamp(event_at, field="event_at")
    if event_type not in {"DECISION_CREATED", "DECISION_RECORDED"}:
        raise DynamicV3ConfirmationCycleError(
            f"owner decision event type invalid: {event_type}"
        )
    payload = {
        "schema_version": RULE_OWNER_DECISION_EVENT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_rule_owner_decision_event",
        "event_type": event_type,
        "event_id": _stable_id(
            "rule-owner-decision-event",
            decision_id,
            event_type,
            event_time.isoformat(),
        ),
        "decision_id": decision_id,
        "cycle_id": cycle_id,
        "event_at": event_time.isoformat(),
        "previous_event_sha256": previous_event_sha256,
        **_jsonable(fields),
    }
    payload["event_sha256"] = _rule_owner_decision_payload_sha256(payload)
    return payload


def _materialize_rule_owner_decisions(
    events: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    decision_order: list[str] = []
    cycle_ids: set[str] = set()
    event_ids: set[str] = set()
    previous_hash = "GENESIS"
    previous_time: datetime | None = None
    for index, raw in enumerate(events):
        event = dict(raw)
        event_type = _text(event.get("event_type"))
        decision_id = _text(event.get("decision_id"))
        cycle_id = _text(event.get("cycle_id"))
        event_id = _text(event.get("event_id"))
        event_time = _rule_owner_decision_timestamp(
            event.get("event_at"),
            field="event_at",
        )
        expected_hash = _rule_owner_decision_payload_sha256(
            {key: value for key, value in event.items() if key != "event_sha256"}
        )
        expected_event_id = _stable_id(
            "rule-owner-decision-event",
            decision_id,
            event_type,
            event_time.isoformat(),
        )
        if (
            event.get("schema_version") != RULE_OWNER_DECISION_EVENT_SCHEMA_VERSION
            or event.get("report_type") != "etf_dynamic_v3_rule_owner_decision_event"
            or event_type not in {"DECISION_CREATED", "DECISION_RECORDED"}
            or not decision_id
            or not cycle_id
            or not event_id
            or event_id != expected_event_id
            or event_id in event_ids
            or event.get("previous_event_sha256") != previous_hash
            or event.get("event_sha256") != expected_hash
            or (previous_time is not None and event_time <= previous_time)
            or event.get("policy_change_allowed") is not False
            or event.get("auto_apply") is not False
            or event.get("broker_action_allowed") is not False
            or event.get("broker_action_taken") is not False
            or event.get("production_effect") != "none"
        ):
            raise DynamicV3ConfirmationCycleError(
                f"owner decision event chain invalid at index {index}"
            )
        event_ids.add(event_id)
        previous_hash = expected_hash
        previous_time = event_time
        if event_type == "DECISION_CREATED":
            target_ids = _texts(event.get("target_ids"))
            allowed = _texts(event.get("allowed_owner_decisions"))
            if (
                decision_id in records
                or cycle_id in cycle_ids
                or not target_ids
                or len(target_ids) != len(set(target_ids))
                or any(not target_id for target_id in target_ids)
                or not allowed
                or len(allowed) != len(set(allowed))
                or any(item not in FINAL_OWNER_DECISIONS for item in allowed)
                or not _text(event.get("decision_scope_reason"))
                or not _text(event.get("recommended_cycle_action"))
                or event.get("owner_decision") != "pending"
                or not _text(event.get("source_snapshot_path"))
                or len(_text(event.get("source_snapshot_sha256"))) != 64
            ):
                raise DynamicV3ConfirmationCycleError(
                    f"owner decision create event invalid: {decision_id}"
                )
            cycle_ids.add(cycle_id)
            decision_order.append(decision_id)
            records[decision_id] = {
                "schema_version": RULE_OWNER_DECISION_RECORD_SCHEMA_VERSION,
                "report_type": "etf_dynamic_v3_rule_owner_decision_record",
                "decision_id": decision_id,
                "cycle_id": cycle_id,
                "created_at": event_time.isoformat(),
                "updated_at": event_time.isoformat(),
                "target_ids": target_ids,
                "decision_scope_reason": _text(event.get("decision_scope_reason")),
                "recommended_cycle_action": _text(event.get("recommended_cycle_action")),
                "allowed_owner_decisions": allowed,
                "owner_decision": "pending",
                "notes": "",
                "source_snapshot_path": _text(event.get("source_snapshot_path")),
                "source_snapshot_sha256": _text(event.get("source_snapshot_sha256")),
                "event_count": 1,
                "event_ids": [event_id],
                "latest_event_id": event_id,
                "latest_event_sha256": expected_hash,
                "policy_change_allowed": False,
                "auto_apply": False,
                "broker_action_allowed": False,
                "broker_action_taken": False,
                "production_effect": "none",
            }
        else:
            record = records.get(decision_id)
            decision = _text(event.get("owner_decision"))
            if (
                record is None
                or record.get("cycle_id") != cycle_id
                or record.get("owner_decision") != "pending"
                or decision not in _texts(record.get("allowed_owner_decisions"))
                or event.get("source_snapshot_path") != record.get("source_snapshot_path")
                or event.get("source_snapshot_sha256")
                != record.get("source_snapshot_sha256")
            ):
                raise DynamicV3ConfirmationCycleError(
                    f"owner decision record event invalid: {decision_id}"
                )
            record["owner_decision"] = decision
            record["notes"] = str(event.get("notes") or "")
            record["updated_at"] = event_time.isoformat()
            record["event_count"] = _int(record.get("event_count")) + 1
            record["event_ids"] = [*_texts(record.get("event_ids")), event_id]
            record["latest_event_id"] = event_id
            record["latest_event_sha256"] = expected_hash
    return [records[decision_id] for decision_id in decision_order]


def _read_rule_owner_decision_source_snapshot(
    *,
    journal_path: Path,
    decision_id: str,
) -> dict[str, Any]:
    path = (
        journal_path.parent
        / decision_id
        / "rule_owner_decision_source_snapshot.json"
    )
    payload = _read_json(path)
    if payload.get("schema_version") != RULE_OWNER_DECISION_SOURCE_SNAPSHOT_SCHEMA_VERSION:
        raise DynamicV3ConfirmationCycleError(
            f"owner decision source snapshot schema invalid: {path}"
        )
    return payload


def _rule_owner_decision_manifest(record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": RULE_OWNER_DECISION_MANIFEST_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_rule_owner_decision_manifest",
        "decision_id": record.get("decision_id"),
        "cycle_id": record.get("cycle_id"),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        "status": "PENDING" if record.get("owner_decision") == "pending" else "RECORDED",
        "owner_decision": record.get("owner_decision"),
        "target_count": len(_texts(record.get("target_ids"))),
        "event_count": record.get("event_count"),
        "latest_event_sha256": record.get("latest_event_sha256"),
        "source_snapshot_sha256": record.get("source_snapshot_sha256"),
        "policy_change_allowed": False,
        "auto_apply": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
    }


def _write_rule_owner_decision_views(
    *,
    journal_path: Path,
    events: Sequence[Mapping[str, Any]],
    records: Sequence[Mapping[str, Any]],
    record: Mapping[str, Any],
    source_snapshot: Mapping[str, Any],
) -> None:
    decision_id = _text(record.get("decision_id"))
    decision_dir = journal_path.parent / decision_id
    write_json_atomic(
        decision_dir / "rule_owner_decision_source_snapshot.json",
        _jsonable(source_snapshot),
    )
    write_json_atomic(
        decision_dir / "rule_owner_decision_record.json",
        _jsonable(record),
    )
    write_json_atomic(
        decision_dir / "rule_owner_decision_manifest.json",
        _rule_owner_decision_manifest(record),
    )
    write_text_atomic(
        decision_dir / "rule_owner_decision_report.md",
        render_rule_owner_decision_detail_report(record, source_snapshot),
    )
    write_text_atomic(journal_path, _rule_owner_decision_jsonl_text(events))
    write_text_atomic(
        journal_path.parent / "rule_owner_decision_report.md",
        render_rule_owner_decision_report(records),
    )


def _rule_owner_decision_payload_sha256(payload: Mapping[str, Any]) -> str:
    material = json.dumps(
        _jsonable(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def _rule_owner_decision_json_text(payload: Mapping[str, Any]) -> str:
    return json.dumps(
        _jsonable(payload),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ) + "\n"


def _rule_owner_decision_jsonl_text(rows: Sequence[Mapping[str, Any]]) -> str:
    return "".join(
        json.dumps(_jsonable(row), ensure_ascii=False, sort_keys=True) + "\n"
        for row in rows
    )


def _texts(value: Any) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        return []
    return [_text(item) for item in value]


def _select_owner_decision(
    records: Sequence[Mapping[str, Any]], *, decision_id: str | None, latest: bool
) -> dict[str, Any]:
    if decision_id is not None and latest:
        raise DynamicV3ConfirmationCycleError("--decision-id and --latest cannot be combined")
    if decision_id is not None:
        for row in records:
            if row.get("decision_id") == decision_id:
                return dict(row)
        raise DynamicV3ConfirmationCycleError(f"owner decision not found: {decision_id}")
    return dict(records[-1])


def _write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(_jsonable(payload), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise DynamicV3ConfirmationCycleError(f"required YAML artifact not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, Mapping):
        raise DynamicV3ConfirmationCycleError(f"YAML artifact root must be mapping: {path}")
    return dict(raw)


def _read_yaml_optional(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return _read_yaml(path)


def _read_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    try:
        json.dumps(value)
    except TypeError:
        return str(value)
    return value
